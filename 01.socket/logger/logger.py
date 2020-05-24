import logging
import inspect
import time


class LoggerBuidler:
    _logger = None

    def __init__(
        self, name, level=logging.DEBUG, format="[%(levelname)s][%(asctime)s] %(message)s"
    ):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

        self.formatter = logging.Formatter(format)

    def build(self) -> logging.Logger:
        return self._logger

    def addFileHandler(self, path, level=logging.DEBUG, *args, **kwargs) -> None:
        fh = logging.FileHandler(path)
        fh.setLevel(level)
        fh.setFormatter(self.formatter)
        self._logger.addHandler(fh)

    def addStreamHandler(self, level=logging.DEBUG) -> None:
        sh = logging.StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(self.formatter)
        self._logger.addHandler(sh)


class ClassLogger(logging.getLoggerClass()):
    """ 기존 Logger 클래스와 통일성 위해 매서드 이름은 camelCase로 작성 """

    def __init__(self, name="", *args, **kwargs):
        if not name:
            name = getattr(inspect.getmodule(self.__class__), "__name__")
        super(ClassLogger, self).__init__(name, *args, **kwargs)

    @staticmethod
    def debug_mode(func):
        def inner(*args, **kwargs):
            try:
                cls = args[0]
                cls_name = getattr(cls.__class__, "__name__")
                module = getattr(cls.__class__, "__module__")
                logger = logging.getLogger(name=module)
            except (IndexError, AttributeError):
                logger = logging.getLogger()

            stime = time.time()
            try:
                func_name = f"{cls_name}.{getattr(func, '__name__')}"
                logger.debug(f"{func_name} start")

                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    logger.error(e)
                    raise e()

                logger.debug(f"{func_name} end, running time: {time.time() - stime}")
                return result

            except Exception as e:
                logger.error(f"{func_name} {e}")

        return inner


logging.setLoggerClass(ClassLogger)
