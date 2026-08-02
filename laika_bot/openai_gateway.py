from __future__ import annotations

import inspect
import logging
import re
from dataclasses import dataclass
from enum import StrEnum
from time import perf_counter
from typing import Any

import openai
from openai import AsyncOpenAI

from .ai_comment_generation import (
    AISingleCommentContext,
    AISingleCommentOutput,
    single_comment_input,
    single_comment_instructions,
)
from .ai_dialogues import (
    AIDialogueReplyContext,
    dialogue_reply_input,
    dialogue_reply_instructions,
)
from .config import Settings

logger = logging.getLogger("laika_bot.openai_gateway")

_REQUEST_ID_RE = re.compile(r"[A-Za-z0-9_.:-]{1,160}")
_MODEL_RE = re.compile(r"[A-Za-z0-9_.:-]{1,96}")


class OpenAIErrorClass(StrEnum):
    DISABLED = "disabled"
    CONFIGURATION = "configuration"
    AUTHENTICATION = "authentication"
    PERMISSION = "permission"
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    BAD_REQUEST = "bad_request"
    NOT_FOUND = "not_found"
    SERVER = "server"
    API = "api"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class OpenAIGatewayStatus:
    railway_enabled: bool
    key_configured: bool
    model: str
    timeout_seconds: float
    max_retries: int
    sdk_version: str

    @property
    def ready(self) -> bool:
        return self.railway_enabled and self.key_configured and bool(self.model)


@dataclass(frozen=True, slots=True)
class OpenAIProbeResult:
    success: bool
    model_name: str
    request_id_safe: str | None
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    latency_ms: int
    error_class: str | None = None


@dataclass(frozen=True, slots=True)
class OpenAICommentGenerationResult:
    output: AISingleCommentOutput
    model_name: str
    request_id_safe: str | None
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    latency_ms: int


class OpenAIGatewayError(RuntimeError):
    def __init__(self, safe_message: str, result: OpenAIProbeResult) -> None:
        super().__init__(safe_message)
        self.safe_message = safe_message
        self.result = result


def _safe_request_id(value: object) -> str | None:
    text = str(value or "").strip()
    if not text or _REQUEST_ID_RE.fullmatch(text) is None:
        return None
    return text[:160]


def _safe_model_name(value: object, *, fallback: str) -> str:
    text = str(value or "").strip()
    if _MODEL_RE.fullmatch(text) is None:
        return fallback
    return text


def _non_negative_int(value: object) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def _usage_value(usage: object, name: str) -> int:
    return _non_negative_int(getattr(usage, name, 0))


def _cached_tokens(usage: object) -> int:
    details = getattr(usage, "input_tokens_details", None)
    return _non_negative_int(getattr(details, "cached_tokens", 0))


class OpenAIGateway:
    """Fail-closed Responses API gateway for DEV probes and explicit Step 11 drafts."""

    def __init__(self, settings: Settings, *, client: Any | None = None) -> None:
        self._settings = settings
        self._client = client
        self._owns_client = client is None

    @property
    def status(self) -> OpenAIGatewayStatus:
        return OpenAIGatewayStatus(
            railway_enabled=bool(self._settings.openai_gateway_enabled),
            key_configured=self._settings.openai_api_key is not None,
            model=self._settings.openai_model,
            timeout_seconds=float(self._settings.openai_request_timeout_seconds),
            max_retries=int(self._settings.openai_max_retries),
            sdk_version=str(getattr(openai, "__version__", "unknown")),
        )

    def _ensure_client(self) -> Any:
        status = self.status
        if not status.railway_enabled:
            raise self._configuration_error(
                OpenAIErrorClass.DISABLED,
                "OpenAI Gateway выключен переменной Railway.",
            )
        if not status.key_configured:
            raise self._configuration_error(
                OpenAIErrorClass.CONFIGURATION,
                "OPENAI_API_KEY не настроен в Railway.",
            )
        if self._client is None:
            api_key = self._settings.openai_api_key_value()
            self._client = AsyncOpenAI(
                api_key=api_key,
                timeout=float(self._settings.openai_request_timeout_seconds),
                max_retries=int(self._settings.openai_max_retries),
            )
        return self._client

    def _configuration_error(
        self,
        error_class: OpenAIErrorClass,
        message: str,
    ) -> OpenAIGatewayError:
        return OpenAIGatewayError(
            message,
            OpenAIProbeResult(
                success=False,
                model_name=self._settings.openai_model,
                request_id_safe=None,
                input_tokens=0,
                output_tokens=0,
                cached_tokens=0,
                latency_ms=0,
                error_class=error_class.value,
            ),
        )

    async def run_dev_probe(self) -> OpenAIProbeResult:
        """Make one tiny fixed Responses API call without Telegram/user content."""

        started = perf_counter()
        client = self._ensure_client()
        try:
            response = await client.responses.create(
                model=self._settings.openai_model,
                instructions=(
                    "This is an infrastructure connectivity probe. "
                    "Return exactly the ASCII token OK and nothing else."
                ),
                input="Return exactly OK.",
                max_output_tokens=32,
                store=False,
            )
        except Exception as exc:  # noqa: BLE001 - mapped to safe public classes below
            result, message = self._map_exception(exc, started)
            logger.warning(
                "OpenAI DEV probe failed class=%s model=%s request_id=%s latency_ms=%s",
                result.error_class,
                result.model_name,
                result.request_id_safe,
                result.latency_ms,
            )
            raise OpenAIGatewayError(message, result) from exc

        latency_ms = max(0, round((perf_counter() - started) * 1000))
        usage = getattr(response, "usage", None)
        result = OpenAIProbeResult(
            success=True,
            model_name=_safe_model_name(
                getattr(response, "model", None),
                fallback=self._settings.openai_model,
            ),
            request_id_safe=_safe_request_id(getattr(response, "_request_id", None)),
            input_tokens=_usage_value(usage, "input_tokens"),
            output_tokens=_usage_value(usage, "output_tokens"),
            cached_tokens=_cached_tokens(usage),
            latency_ms=latency_ms,
            error_class=None,
        )
        if str(getattr(response, "output_text", "")).strip() != "OK":
            failed = OpenAIProbeResult(
                success=False,
                model_name=result.model_name,
                request_id_safe=result.request_id_safe,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                cached_tokens=result.cached_tokens,
                latency_ms=result.latency_ms,
                error_class=OpenAIErrorClass.API.value,
            )
            logger.warning(
                "OpenAI DEV probe returned unexpected control output model=%s request_id=%s latency_ms=%s",
                failed.model_name,
                failed.request_id_safe,
                failed.latency_ms,
            )
            raise OpenAIGatewayError(
                "OpenAI ответил, но контрольный ответ не прошёл проверку.",
                failed,
            )
        logger.info(
            "OpenAI DEV probe completed model=%s request_id=%s latency_ms=%s "
            "input_tokens=%s output_tokens=%s cached_tokens=%s",
            result.model_name,
            result.request_id_safe,
            result.latency_ms,
            result.input_tokens,
            result.output_tokens,
            result.cached_tokens,
        )
        return result

    async def generate_single_comment(
        self,
        context: AISingleCommentContext,
        *,
        max_output_tokens: int = 512,
    ) -> OpenAICommentGenerationResult:
        """Generate one strict draft/skip result; never publish or store source text."""

        if not 128 <= int(max_output_tokens) <= 2_048:
            raise ValueError("max_output_tokens должен быть от 128 до 2048")
        started = perf_counter()
        client = self._ensure_client()
        try:
            response = await client.responses.parse(
                model=self._settings.openai_model,
                instructions=single_comment_instructions(context),
                input=single_comment_input(context),
                text_format=AISingleCommentOutput,
                max_output_tokens=int(max_output_tokens),
                store=False,
            )
            parsed = getattr(response, "output_parsed", None)
            if not isinstance(parsed, AISingleCommentOutput):
                raw = str(getattr(response, "output_text", ""))
                parsed = AISingleCommentOutput.model_validate_json(raw)
        except Exception as exc:  # noqa: BLE001 - classified and redacted below
            result, message = self._map_exception(exc, started)
            logger.warning(
                "OpenAI single draft failed class=%s model=%s request_id=%s latency_ms=%s",
                result.error_class,
                result.model_name,
                result.request_id_safe,
                result.latency_ms,
            )
            raise OpenAIGatewayError(message, result) from exc

        latency_ms = max(0, round((perf_counter() - started) * 1000))
        usage = getattr(response, "usage", None)
        result = OpenAICommentGenerationResult(
            output=parsed,
            model_name=_safe_model_name(
                getattr(response, "model", None),
                fallback=self._settings.openai_model,
            ),
            request_id_safe=_safe_request_id(getattr(response, "_request_id", None)),
            input_tokens=_usage_value(usage, "input_tokens"),
            output_tokens=_usage_value(usage, "output_tokens"),
            cached_tokens=_cached_tokens(usage),
            latency_ms=latency_ms,
        )
        logger.info(
            "OpenAI single draft completed model=%s request_id=%s latency_ms=%s "
            "input_tokens=%s output_tokens=%s cached_tokens=%s decision=%s",
            result.model_name,
            result.request_id_safe,
            result.latency_ms,
            result.input_tokens,
            result.output_tokens,
            result.cached_tokens,
            result.output.decision,
        )
        return result

    async def generate_dialogue_reply(
        self,
        context: AIDialogueReplyContext,
        *,
        max_output_tokens: int = 512,
    ) -> OpenAICommentGenerationResult:
        """Generate one finite dialogue reply as a strict draft/skip result."""

        if not 128 <= int(max_output_tokens) <= 2_048:
            raise ValueError("max_output_tokens должен быть от 128 до 2048")
        started = perf_counter()
        client = self._ensure_client()
        try:
            response = await client.responses.parse(
                model=self._settings.openai_model,
                instructions=dialogue_reply_instructions(context),
                input=dialogue_reply_input(context),
                text_format=AISingleCommentOutput,
                max_output_tokens=int(max_output_tokens),
                store=False,
            )
            parsed = getattr(response, "output_parsed", None)
            if not isinstance(parsed, AISingleCommentOutput):
                raw = str(getattr(response, "output_text", ""))
                parsed = AISingleCommentOutput.model_validate_json(raw)
        except Exception as exc:  # noqa: BLE001
            result, message = self._map_exception(exc, started)
            logger.warning(
                "OpenAI dialogue reply failed class=%s model=%s request_id=%s latency_ms=%s thread=%s position=%s",
                result.error_class,
                result.model_name,
                result.request_id_safe,
                result.latency_ms,
                context.thread.id,
                context.position,
            )
            raise OpenAIGatewayError(message, result) from exc

        latency_ms = max(0, round((perf_counter() - started) * 1000))
        usage = getattr(response, "usage", None)
        result = OpenAICommentGenerationResult(
            output=parsed,
            model_name=_safe_model_name(
                getattr(response, "model", None),
                fallback=self._settings.openai_model,
            ),
            request_id_safe=_safe_request_id(getattr(response, "_request_id", None)),
            input_tokens=_usage_value(usage, "input_tokens"),
            output_tokens=_usage_value(usage, "output_tokens"),
            cached_tokens=_cached_tokens(usage),
            latency_ms=latency_ms,
        )
        logger.info(
            "OpenAI dialogue reply completed model=%s request_id=%s latency_ms=%s "
            "input_tokens=%s output_tokens=%s cached_tokens=%s decision=%s thread=%s position=%s",
            result.model_name,
            result.request_id_safe,
            result.latency_ms,
            result.input_tokens,
            result.output_tokens,
            result.cached_tokens,
            result.output.decision,
            context.thread.id,
            context.position,
        )
        return result

    def _map_exception(
        self,
        exc: Exception,
        started: float,
    ) -> tuple[OpenAIProbeResult, str]:
        latency_ms = max(0, round((perf_counter() - started) * 1000))
        request_id = _safe_request_id(getattr(exc, "request_id", None))
        status_code = getattr(exc, "status_code", None)

        if isinstance(exc, openai.AuthenticationError):
            error_class = OpenAIErrorClass.AUTHENTICATION
            message = "OpenAI отклонил API-ключ. Проверь OPENAI_API_KEY."
        elif isinstance(exc, openai.PermissionDeniedError):
            error_class = OpenAIErrorClass.PERMISSION
            message = "Проект или ключ не имеет доступа к выбранной модели."
        elif isinstance(exc, openai.RateLimitError):
            error_class = OpenAIErrorClass.RATE_LIMIT
            message = "OpenAI временно ограничил запрос или исчерпан доступный лимит."
        elif isinstance(exc, openai.APITimeoutError):
            error_class = OpenAIErrorClass.TIMEOUT
            message = "OpenAI не ответил до установленного тайм-аута."
        elif isinstance(exc, openai.APIConnectionError):
            error_class = OpenAIErrorClass.CONNECTION
            message = "Не удалось установить безопасное соединение с OpenAI."
        elif isinstance(exc, (openai.BadRequestError, openai.UnprocessableEntityError)):
            error_class = OpenAIErrorClass.BAD_REQUEST
            message = "OpenAI отклонил тестовый запрос или параметры модели."
        elif isinstance(exc, openai.NotFoundError):
            error_class = OpenAIErrorClass.NOT_FOUND
            message = "Выбранная модель не найдена или недоступна проекту."
        elif isinstance(exc, openai.InternalServerError) or (
            isinstance(status_code, int) and status_code >= 500
        ):
            error_class = OpenAIErrorClass.SERVER
            message = "На стороне OpenAI произошла временная серверная ошибка."
        elif isinstance(exc, openai.APIError):
            error_class = OpenAIErrorClass.API
            message = "OpenAI вернул безопасно классифицированную API-ошибку."
        else:
            error_class = OpenAIErrorClass.UNKNOWN
            message = "Тест OpenAI завершился неизвестной безопасно скрытой ошибкой."

        return (
            OpenAIProbeResult(
                success=False,
                model_name=self._settings.openai_model,
                request_id_safe=request_id,
                input_tokens=0,
                output_tokens=0,
                cached_tokens=0,
                latency_ms=latency_ms,
                error_class=error_class.value,
            ),
            message,
        )

    async def close(self) -> None:
        if self._client is None or not self._owns_client:
            return
        client, self._client = self._client, None
        close = getattr(client, "close", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result
