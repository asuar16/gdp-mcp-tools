"""Gemini CLI integration tools for cross-model collaboration.

Allows Claude Code to call Gemini CLI for second opinions, reviews,
and collaborative problem-solving within the same conversation.

Requires: gemini CLI installed (npm install -g @google/gemini-cli)
"""

import json
import logging
import subprocess

logger = logging.getLogger(__name__)


def _run_gemini(prompt, cwd=None, timeout=180):
    """Execute Gemini CLI in print mode and return the response."""
    try:
        result = subprocess.run(
            ["gemini", "-p", prompt],
            capture_output=True, text=True, timeout=timeout, cwd=cwd,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return f"Gemini error (exit {result.returncode}): {result.stderr.strip()}"
    except subprocess.TimeoutExpired:
        return f"Gemini timed out after {timeout}s. Try a simpler prompt."
    except FileNotFoundError:
        return "Gemini CLI not found. Install: npm install -g @google/gemini-cli"


def register(mcp):

    @mcp.tool()
    def gemini_review(prompt: str, context: str = "") -> str:
        """Ask Gemini CLI for a second opinion or review.

        Use this when you want a different model's perspective on:
        - Code design decisions
        - Schema designs
        - Edge cases you might have missed
        - Alternative approaches

        Args:
            prompt: The question or review request for Gemini
            context: Optional context to include (code snippet, design doc, etc.)
        """
        full_prompt = prompt
        if context:
            full_prompt = f"{prompt}\n\nContext:\n{context}"

        logger.info("Calling Gemini CLI for review...")
        response = _run_gemini(full_prompt)
        return json.dumps({"gemini_response": response})

    @mcp.tool()
    def gemini_debate(topic: str, my_position: str) -> str:
        """Start a debate with Gemini on a technical topic.

        Present your position and get Gemini's counterarguments.
        Use this to stress-test your design decisions.

        Args:
            topic: The debate topic (e.g. "should we use COLLECT_SET for cs_session_ids")
            my_position: Your current position/proposal that Gemini should critique
        """
        debate_prompt = (
            f"You are reviewing a technical proposal. Be critical but constructive.\n\n"
            f"TOPIC: {topic}\n\n"
            f"PROPOSAL:\n{my_position}\n\n"
            f"Critique this proposal:\n"
            f"1. What's wrong or risky?\n"
            f"2. What edge cases are missed?\n"
            f"3. What would you do differently?\n"
            f"4. What do you agree with?\n"
            f"Be specific and concise. Max 300 words."
        )

        logger.info("Calling Gemini CLI for debate...")
        response = _run_gemini(debate_prompt)
        return json.dumps({"topic": topic, "gemini_critique": response})