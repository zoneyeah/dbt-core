
from structlog import get_logger  # type: ignore # TODO eventually remove dependency on this logger
from dbt.events.history import EVENT_HISTORY
from dbt.events.types import CliEventABC, Event, ShowException


log = get_logger()


def timestamped_cli_line(e: CliEventABC) -> str:
    ts = e.ts.strftime("%H:%M:%S")
    msg = e.cli_msg()
    return f"{ts} | {msg}"


# top-level method for accessing the new eventing system
# this is where all the side effects happen branched by event type
# (i.e. - mutating the event history, printing to stdout, logging
# to files, etc.)
def fire_event(e: Event) -> None:
    EVENT_HISTORY.append(e)
    if isinstance(e, CliEventABC):
        if e.level_tag() == 'test' and not isinstance(e, ShowException):
            # TODO after implmenting #3977 send to new test level
            log.debug(timestamped_cli_line(e))
        elif e.level_tag() == 'test' and isinstance(e, ShowException):
            # TODO after implmenting #3977 send to new test level
            log.debug(
                timestamped_cli_line(e),
                exc_info=e.exc_info,
                stack_info=e.stack_info,
                extra=e.extra
            )
        elif e.level_tag() == 'debug' and not isinstance(e, ShowException):
            log.debug(timestamped_cli_line(e))
        elif e.level_tag() == 'debug' and isinstance(e, ShowException):
            log.debug(
                timestamped_cli_line(e),
                exc_info=e.exc_info,
                stack_info=e.stack_info,
                extra=e.extra
            )
        elif e.level_tag() == 'info' and not isinstance(e, ShowException):
            log.info(timestamped_cli_line(e))
        elif e.level_tag() == 'info' and isinstance(e, ShowException):
            log.info(
                timestamped_cli_line(e),
                exc_info=e.exc_info,
                stack_info=e.stack_info,
                extra=e.extra
            )
        elif e.level_tag() == 'warn' and not isinstance(e, ShowException):
            log.warning(timestamped_cli_line(e))
        elif e.level_tag() == 'warn' and isinstance(e, ShowException):
            log.warning(
                timestamped_cli_line(e),
                exc_info=e.exc_info,
                stack_info=e.stack_info,
                extra=e.extra
            )
        elif e.level_tag() == 'error' and not isinstance(e, ShowException):
            log.error(timestamped_cli_line(e))
        elif e.level_tag() == 'error' and isinstance(e, ShowException):
            log.error(
                timestamped_cli_line(e),
                exc_info=e.exc_info,
                stack_info=e.stack_info,
                extra=e.extra
            )
        else:
            raise AssertionError(
                f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
            )
