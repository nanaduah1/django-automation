import time

from django.conf import settings
from django.core.management.base import BaseCommand

from automation import common

logger = settings.LOGGER
CONFIG = settings.CONFIG_PROVIDER


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Automation is starting")
        engine = common.WorkerEngine()
        try:
            engine.initialize_jobs()
            while True:
                time.sleep(int(CONFIG.get("SCHEDULER_RUN_INTERVAL_SECONDS", "10")))
                try:
                    engine.run_automations()
                except KeyboardInterrupt:
                    break
                except Exception as ex:
                    print(ex)
                    logger.exception(ex)
        except KeyboardInterrupt:
            logger.info("Automation is stopping")
