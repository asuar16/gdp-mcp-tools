"""VPN tools for GDP MCP server.

Manages the F5 VPN connection via the macOS F5 VPN app.
Provides connect, disconnect, status, and auto-reconnect watchdog.
"""

import json
import logging
import os
import subprocess
import threading
import time
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

F5_APP_NAME = "F5 VPN"
F5_APP_PATH = f"/Applications/{F5_APP_NAME}.app"
VPN_PORTAL_URL = os.environ.get("VPN_PORTAL_URL", "https://vpn.grubhub.com")

# Watchdog settings
WATCHDOG_INTERVAL_SEC = 30 * 60  # 30 minutes - reconnect check
KEEPALIVE_INTERVAL_SEC = 2 * 60  # 2 minutes - ping to prevent idle disconnect
IST = timezone(timedelta(hours=5, minutes=30))
WATCHDOG_START_HOUR = 9   # 9:00 AM IST
WATCHDOG_END_HOUR = 21    # 9:00 PM IST

# Internal hostnames to probe for connectivity checks
_INTERNAL_PROBE_HOSTS = [
    "dev-jenkins.gdp.data.grubhub.com",
    "dev-azkaban.gdp.data.grubhub.com",
]

# Watchdog state
_watchdog_thread = None
_watchdog_stop = threading.Event()

# Keepalive state
_keepalive_thread = None
_keepalive_stop = threading.Event()


def _run_cmd(cmd, timeout=10):
    """Run a shell command and return (stdout, stderr, returncode)."""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )
        return result.stdout.strip(), result.stderr.strip(), result.returncode
    except subprocess.TimeoutExpired:
        return "", "timeout", -1
    except FileNotFoundError:
        return "", "command not found", -1


def _is_f5_running():
    """Check if the F5 VPN app process is running."""
    stdout, _, rc = _run_cmd(["pgrep", "-f", F5_APP_NAME])
    return rc == 0 and bool(stdout)


def _has_vpn_tunnel():
    """Check for active VPN tunnel interfaces (utun)."""
    stdout, _, _ = _run_cmd(["ifconfig"])
    # Count utun interfaces with inet addresses (active tunnels)
    active_tunnels = []
    current_iface = None
    for line in stdout.split("\n"):
        if line and not line.startswith("\t") and not line.startswith(" "):
            current_iface = line.split(":")[0]
        elif current_iface and current_iface.startswith("utun") and "inet " in line:
            active_tunnels.append(current_iface)
    return len(active_tunnels) > 0, active_tunnels


def _can_reach_internal():
    """Try to reach an internal GDP host to confirm VPN connectivity."""
    for host in _INTERNAL_PROBE_HOSTS:
        stdout, _, rc = _run_cmd(
            ["ping", "-c", "1", "-W", "2", host], timeout=5
        )
        if rc == 0:
            return True, host
    return False, None


def _is_business_hours():
    """Check if current time is weekday 9 AM - 9 PM IST."""
    now_ist = datetime.now(IST)
    # Monday=0, Sunday=6
    if now_ist.weekday() >= 5:
        return False
    return WATCHDOG_START_HOUR <= now_ist.hour < WATCHDOG_END_HOUR


def _keepalive_loop():
    """Background loop: ping internal host every 2 min to prevent VPN idle disconnect."""
    logger.info("VPN keepalive started (every %ds)", KEEPALIVE_INTERVAL_SEC)

    while not _keepalive_stop.wait(KEEPALIVE_INTERVAL_SEC):
        if not _is_business_hours():
            continue
        # Silent ping - just generate traffic to keep VPN session alive
        _run_cmd(["ping", "-c", "1", "-W", "2", _INTERNAL_PROBE_HOSTS[0]], timeout=5)


def _watchdog_loop():
    """Background loop: check VPN every 30 min during business hours, auto-reopen browser if down."""
    logger.info("VPN watchdog started (every %ds, weekdays %d:00-%d:00 IST)",
                WATCHDOG_INTERVAL_SEC, WATCHDOG_START_HOUR, WATCHDOG_END_HOUR)

    while not _watchdog_stop.wait(WATCHDOG_INTERVAL_SEC):
        if not _is_business_hours():
            logger.debug("Watchdog: outside business hours, skipping check")
            continue

        reachable, _ = _can_reach_internal()
        if reachable:
            logger.debug("Watchdog: VPN is connected")
            continue

        # VPN is down during business hours - auto-reopen browser
        logger.warning("Watchdog: VPN disconnected during business hours, opening %s", VPN_PORTAL_URL)
        _run_cmd(["open", VPN_PORTAL_URL])


def _start_watchdog():
    """Start the watchdog and keepalive background threads."""
    global _watchdog_thread, _keepalive_thread

    started = False

    if _watchdog_thread is None or not _watchdog_thread.is_alive():
        _watchdog_stop.clear()
        _watchdog_thread = threading.Thread(target=_watchdog_loop, daemon=True, name="vpn-watchdog")
        _watchdog_thread.start()
        started = True

    if _keepalive_thread is None or not _keepalive_thread.is_alive():
        _keepalive_stop.clear()
        _keepalive_thread = threading.Thread(target=_keepalive_loop, daemon=True, name="vpn-keepalive")
        _keepalive_thread.start()
        started = True

    return started


def _stop_watchdog():
    """Stop the watchdog and keepalive background threads."""
    global _watchdog_thread, _keepalive_thread

    stopped = False

    if _keepalive_thread is not None and _keepalive_thread.is_alive():
        _keepalive_stop.set()
        _keepalive_thread.join(timeout=5)
        _keepalive_thread = None
        stopped = True

    if _watchdog_thread is not None and _watchdog_thread.is_alive():
        _watchdog_stop.set()
        _watchdog_thread.join(timeout=5)
        _watchdog_thread = None
        stopped = True

    return stopped


def register(mcp):

    @mcp.tool()
    def vpn_status() -> str:
        """Check the current VPN connection status.

        Checks whether the F5 VPN app is running, a VPN tunnel is active,
        and internal GDP hosts are reachable.
        """
        f5_running = _is_f5_running()
        has_tunnel, tunnels = _has_vpn_tunnel()
        reachable, reached_host = _can_reach_internal()
        watchdog_active = _watchdog_thread is not None and _watchdog_thread.is_alive()
        keepalive_active = _keepalive_thread is not None and _keepalive_thread.is_alive()

        if reachable:
            status = "CONNECTED"
            message = f"VPN is connected. Internal host {reached_host} is reachable."
        elif has_tunnel:
            status = "TUNNEL_UP"
            message = f"VPN tunnel exists ({', '.join(tunnels)}) but internal hosts are not responding."
        elif f5_running:
            status = "APP_RUNNING"
            message = "F5 VPN app is running but no active tunnel detected. You may need to authenticate."
        else:
            status = "DISCONNECTED"
            message = "VPN is not connected. F5 VPN app is not running."

        return json.dumps({
            "status": status,
            "f5_app_running": f5_running,
            "vpn_tunnel_active": has_tunnel,
            "tunnel_interfaces": tunnels if has_tunnel else [],
            "internal_reachable": reachable,
            "watchdog_active": watchdog_active,
            "keepalive_active": keepalive_active,
            "message": message,
        })

    @mcp.tool()
    def vpn_connect() -> str:
        """Start the F5 VPN connection.

        Launches the F5 VPN app. You will need to authenticate via Okta
        in the browser window that opens. After authentication, the VPN
        tunnel will be established automatically.
        """
        # Check if already connected
        reachable, reached_host = _can_reach_internal()
        if reachable:
            # Start watchdog even if already connected
            _start_watchdog()
            return json.dumps({
                "status": "ALREADY_CONNECTED",
                "watchdog_active": True,
                "message": f"VPN is already connected. {reached_host} is reachable. Watchdog is active.",
            })

        # Open the VPN portal URL in the default browser.
        # Flow: browser -> Okta auth -> portal redirects via f5-vpn:// scheme -> F5 VPN app connects
        _, stderr, rc = _run_cmd(["open", VPN_PORTAL_URL])
        if rc != 0:
            return json.dumps({
                "error": f"Failed to open VPN portal: {stderr}",
            })

        # Poll for connection (user needs to authenticate via Okta in browser)
        # Give the user up to 120 seconds to complete Okta auth
        deadline = time.time() + 120
        while time.time() < deadline:
            reachable, reached_host = _can_reach_internal()
            if reachable:
                _start_watchdog()
                return json.dumps({
                    "status": "CONNECTED",
                    "watchdog_active": True,
                    "message": (
                        f"VPN connected successfully. {reached_host} is reachable. "
                        f"Watchdog enabled: checks every 30 min (weekdays 9AM-9PM IST), "
                        f"auto-reopens browser if VPN drops."
                    ),
                })
            time.sleep(5)

        # Timed out waiting for auth - still start watchdog
        _start_watchdog()
        f5_running = _is_f5_running()
        return json.dumps({
            "status": "WAITING_FOR_AUTH",
            "f5_app_running": f5_running,
            "watchdog_active": True,
            "message": (
                "VPN is not yet connected. "
                "Please complete Okta authentication in the browser at " + VPN_PORTAL_URL + ". "
                "Watchdog is active and will auto-reopen browser if VPN drops during business hours."
            ),
        })

    @mcp.tool()
    def vpn_disconnect() -> str:
        """Disconnect the F5 VPN.

        Quits the F5 VPN app, which tears down the VPN tunnel.
        """
        # Stop watchdog
        _stop_watchdog()

        if not _is_f5_running():
            return json.dumps({
                "status": "ALREADY_DISCONNECTED",
                "watchdog_active": False,
                "message": "F5 VPN app is not running. Watchdog stopped.",
            })

        # Quit the app gracefully via AppleScript
        _, stderr, rc = _run_cmd([
            "osascript", "-e",
            f'tell application "{F5_APP_NAME}" to quit',
        ])

        if rc != 0:
            # Fallback: kill the process
            logger.warning("AppleScript quit failed, using pkill: %s", stderr)
            _run_cmd(["pkill", "-f", F5_APP_NAME])

        # Verify disconnection
        time.sleep(2)
        reachable, _ = _can_reach_internal()
        f5_running = _is_f5_running()

        if not f5_running and not reachable:
            return json.dumps({
                "status": "DISCONNECTED",
                "watchdog_active": False,
                "message": "VPN disconnected successfully. Watchdog stopped.",
            })

        return json.dumps({
            "status": "DISCONNECTING",
            "f5_still_running": f5_running,
            "internal_still_reachable": reachable,
            "watchdog_active": False,
            "message": "Disconnect requested. Watchdog stopped. App may take a moment to fully stop.",
        })
