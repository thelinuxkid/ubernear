import tempfile
import functools
import shutil

def assert_raises(excClass, callableObj, *args, **kwargs):
    """
    Like unittest.TestCase.assertRaises, but returns the exception.
    """
    try:
        callableObj(*args, **kwargs)
    except excClass, e:
        return e
    else:
        if hasattr(excClass,'__name__'): excName = excClass.__name__
        else: excName = str(excClass)
        raise AssertionError("%s not raised" % excName)

def assert_exceptions_equal(exc1, exc2):
    if type(exc1) != type(exc2):
        raise AssertionError(
            '{exc1} does not equal {exc2}'.format(
                exc1=type(exc1),
                exc2=type(exc2),
                )
            )
    if exc1.args != exc2.args:
        raise AssertionError(
            '{exc1} args do not equal {exc2} args'.format(
                exc1=exc1.args,
                exc2=exc2.args,
                )
            )
    if exc2.message != exc2.message:
        raise AssertionError(
            '"{exc1}" message does not equal "{exc2}" message'.format(
                exc1=exc1.message,
                exc2=exc2.message,
                )
            )

class tmp_dirs(object):
    def __init__(self, num):
        self._num = num

    def __call__(self, fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            def manager():
                try:
                    dirs = [
                        tempfile.mkdtemp()
                        for i in xrange(self._num)
                        ]
                    extended_args = list(args)
                    extended_args += dirs
                    fn(*extended_args, **kwargs)
                finally:
                    for dir_ in dirs:
                        shutil.rmtree(dir_)

            return manager()
        return wrapper
