__all__ = ["get_scheduler_service"]


__scheduler_service = None

def get_scheduler_service():
    global __scheduler_service
    if not __scheduler_service:
        from .scheduler_fifo import SchedulerFIFO as Scheduler
        __scheduler_service = Scheduler(do_monitor=False)
    return __scheduler_service