
from structlog import get_logger
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
    level_tag = e.level_tag()
    if isinstance(e, CliEventABC):
        log_line = timestamped_cli_line(e)
        if isinstance(e, ShowException):
            if level_tag == 'test':
                # TODO after implmenting #3977 send to new test level
                log.debug(
                    log_line,
                    exc_info=e.exc_info,
                    stack_info=e.stack_info,
                    extra=e.extra
                )
            elif level_tag == 'debug':
                log.debug(
                    log_line,
                    exc_info=e.exc_info,
                    stack_info=e.stack_info,
                    extra=e.extra
                )
            elif level_tag == 'info':
                log.info(
                    log_line,
                    exc_info=e.exc_info,
                    stack_info=e.stack_info,
                    extra=e.extra
                )
            elif level_tag == 'warn':
                log.warning(
                    log_line,
                    exc_info=e.exc_info,
                    stack_info=e.stack_info,
                    extra=e.extra
                )

            elif level_tag == 'error':
                log.error(
                    log_line,
                    exc_info=e.exc_info,
                    stack_info=e.stack_info,
                    extra=e.extra
                )
            else:
                raise AssertionError(
                    f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
                )
        # not CliEventABC and not ShowException
        else:
            if level_tag == 'test':
                # TODO after implmenting #3977 send to new test level
                log.debug(log_line)
            elif level_tag == 'debug':
                log.debug(log_line)
            elif level_tag == 'info':
                log.info(log_line)
            elif level_tag == 'warn':
                log.warning(log_line)
            elif level_tag == 'error':
                log.error(log_line)
            else:
                raise AssertionError(
                    f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
                )
