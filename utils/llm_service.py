"""
LLM and embedding providers
"""


from typing import Optional, Any, Dict
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

from utils.config import (
    GENERAL_MODEL,
    GENERAL_PROVIDER,
    STRONG_MODEL,
    STRONG_PROVIDER,
    PROVIDER,
    OPENROUTER_BASE_URL,
    get_api_key,
    EMBEDDING_MODEL
)

def _build_llm(
    model:str, 
    provider: str,
    temperature: float = 0,
    streaming: bool = False,
    max_tokens: Optional[int] = None,
    **kwargs: Any
)-> ChatOpenAI:
    """ build a chatopenai for a provider"""
    llm_kwargs: dict[str,Any] = dict(
        model=model,
        temperature=temperature,
        streaming=streaming,
        max_tokens=max_tokens,
        **kwargs
    )
    if provider == "openrouter":
        llm_kwargs['openai_api_base'] = OPENROUTER_BASE_URL
        llm_kwargs['openai_api_key'] = get_api_key("openrouter")
    elif provider == "groq":
        llm_kwargs["openai_api_base"] = "https://api.groq.com/openai/v1"
        llm_kwargs["openai_api_key"] = get_api_key("groq")
    elif provider == "openai":
        llm_kwargs["openai_api_key"] = get_api_key("openai")

    return ChatOpenAI(**llm_kwargs)


def get_general_llm(temperature: float = 0, **kwargs: Any) -> ChatOpenAI:
    """LLM for routing, grading, generation, and general tasks.

    Model: gpt-4o-mini via OpenRouter.
    Optimised for speed and cost with reliable structured output.
    """
    return _build_llm(GENERAL_MODEL, GENERAL_PROVIDER, temperature=temperature, **kwargs)


def get_strong_llm(temperature: float = 0, **kwargs: Any) -> ChatOpenAI:
    """LLM for entity extraction, entity resolution, and complex reasoning.

    Model: gpt-4o via OpenRouter.
    High accuracy for structured medical entity extraction.
    """
    return _build_llm(STRONG_MODEL, STRONG_PROVIDER, temperature=temperature, **kwargs)


# ========================================
# Embedding Provider
# ========================================

def get_embeddings(
    batch_size: int = 100,
    show_progress: bool = False,
    **kwargs: Any,
) -> OpenAIEmbeddings:
    """Get an OpenAIEmbeddings instance configured for the active provider.

    When PROVIDER=openrouter, requests are routed through the OpenRouter
    unified API so that model IDs resolve correctly.
    """
    llm_kwargs: dict[str, Any] = dict(
        model=EMBEDDING_MODEL,
        show_progress_bar=show_progress,
        **kwargs,
    )

    if PROVIDER == "openrouter":
        llm_kwargs["openai_api_base"] = OPENROUTER_BASE_URL
        llm_kwargs["openai_api_key"] = get_api_key("openrouter")

    return OpenAIEmbeddings(**llm_kwargs)
