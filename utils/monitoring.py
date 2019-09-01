from functools import wraps
from time import time


class Monitoring:

    @staticmethod
    def timing(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            start = time()
            result = f(*args, **kwargs)
            end = time()
            time_elapsed = round(end - start, 4)

            if result["log_txt"]:
                print("{}. Elapsed time: {} sec".format(result["log_txt"], time_elapsed))
            else:
                print("func: {}. Elapsed time: {} sec".format(f.__name__, time_elapsed))
            return result
        return wrapper

#
# if __name__ == "__main__":
#
#     #@timing
#     @timing
#     def test_timing(a, b):
#         for i in range(a):
#             i + a
#         return 1
#
#     # print(work(1, 2))
#     # print(work(22, 11))
#     test_timing(100000, 100)
