import src.logger as log

main_logger = log.app_logger(__name__)

try:
    main_logger.info('Hello')

except Exception as e:
    main_logger.error('An error: %s', e, exc_info=True)
