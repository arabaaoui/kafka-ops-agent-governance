"""
ADK factory — builds real google-adk Agents backed by LiteLLM, and runs them
synchronously from the agents' sync Kafka polling loops.

All providers (openai, anthropic, gemini) are routed through LiteLlm. LiteLLM
normalizes request/response formats across providers, so agent code never
touches a provider-specific SDK or HTTP payload.
"""

import asyncio
import logging
import uuid

from google.adk import Agent, Runner
from google.adk.models.lite_llm import LiteLlm
from google.adk.sessions import InMemorySessionService
from google.genai import types

logger = logging.getLogger(__name__)


def create_llm(provider: str, model: str, api_key: str) -> LiteLlm:
    """Return the LiteLlm model instance for the given provider."""
    if provider == "openai":
        return LiteLlm(model=f"openai/{model}", api_key=api_key)
    if provider == "anthropic":
        return LiteLlm(model=f"anthropic/{model}", api_key=api_key)
    if provider == "gemini":
        return LiteLlm(model=f"gemini/{model}", api_key=api_key)
    raise ValueError(f"Unknown LLM provider: {provider}")


class AdkAgentRunner:
    """
    Wraps a google-adk Agent + Runner + InMemorySessionService.

    Built once at agent startup, then called synchronously once per Kafka
    message via run(). Each call creates a disposable session (no
    conversational memory needed between independent anomalies/tasks) and
    deletes it afterwards so InMemorySessionService doesn't grow unbounded
    across a long-running demo.
    """

    def __init__(self, *, name: str, description: str, instruction: str, tools: list, provider: str, model: str, api_key: str):
        self.app_name = name
        self.agent = Agent(
            model=create_llm(provider, model, api_key),
            name=name,
            description=description,
            instruction=instruction,
            tools=tools,
        )
        self._session_service = InMemorySessionService()
        self._runner = Runner(agent=self.agent, app_name=self.app_name, session_service=self._session_service)

    def run(self, user_prompt: str) -> str:
        """Send user_prompt to the agent and return its final text response."""
        return asyncio.run(self._run_async(user_prompt))

    async def _run_async(self, user_prompt: str) -> str:
        user_id = "agent"
        session_id = str(uuid.uuid4())
        await self._session_service.create_session(app_name=self.app_name, user_id=user_id, session_id=session_id)

        final_text = ""
        try:
            async for event in self._runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=types.Content(role="user", parts=[types.Part(text=user_prompt)]),
            ):
                if event.is_final_response() and event.content and event.content.parts:
                    final_text = event.content.parts[0].text or final_text
        finally:
            await self._session_service.delete_session(app_name=self.app_name, user_id=user_id, session_id=session_id)

        return final_text
