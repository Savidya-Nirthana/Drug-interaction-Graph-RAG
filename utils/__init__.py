"""
Week 11 — Shared utilities (infrastructure only).

Students: you don't need to modify these files.
All teaching code lives in the notebooks.
"""

from utils.config import (
    validate,
    dump,
    get_config,
    get_api_key,
    get_all_models,
    get_chat_model,
    get_embedding_model,
    PROVIDER,
    MODEL_TIER,
    GENERAL_MODEL,
    STRONG_MODEL,
    EMBEDDING_MODEL,
    EMBEDDING_DIM,
    LLM_TEMPERATURE,
    LLM_MAX_TOKENS,
    NEO4J_URI,
    NEO4J_USERNAME,
    NEO4J_PASSWORD,
    NEO4J_DATABASE,
    DATA_DIR,
    RAW_DIR,
    PROCESSED_DIR,
    _PROJECT_ROOT,
)

from utils.llm_services import (
    get_general_llm,
    get_strong_llm,
    get_embeddings,
)

from utils.neo4j_client import (
    Neo4jClient,
    get_neo4j_client,
)

from utils.prompts import (
    EXTRACTION_SYSTEM_PROMPT,
    EXTRACTION_HUMAN_PROMPT,
    ENTITY_EXTRACTION_PROMPT,
    ROUTER_SYSTEM_PROMPT,
    GRADER_SYSTEM_PROMPT,
    GRADER_HUMAN_PROMPT,
    REWRITER_SYSTEM_PROMPT,
    REWRITER_HUMAN_PROMPT,
    GENERATOR_SYSTEM_PROMPT,
    GENERATOR_HUMAN_PROMPT,
    GENERAL_MEDICAL_PROMPT,
    CHITCHAT_RESPONSE,
)
