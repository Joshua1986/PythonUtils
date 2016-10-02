# coding=utf-8
__author__ = 'mu.dong'
import decimal
import logging

logger = logging.getLogger("settlement")


def assert_true(conditions):
    if type(conditions) in (list, tuple):
        result = [bool(ifOrderInfo) for ifOrderInfo in conditions]
        logger.error("The conditions is \n\t{0}".format(result))
        return all(result)
    elif type(conditions) == bool:
        if not conditions: logger.error("The condition is false: \n\t%s" % (conditions))
        return conditions
    else:
        logger.error("Condition's type is wrong, just support list, tuple and boolean now!")


def assert_equels(first_arg, second_arg, msg=''):
    first_arg = first_arg.encode("utf8") if type(first_arg) == unicode else first_arg
    second_arg = second_arg.encode("utf8") if type(second_arg) == unicode else second_arg
    first_arg = float(first_arg) if type(first_arg) == decimal.Decimal else first_arg
    second_arg = float(second_arg) if type(second_arg) == decimal.Decimal else second_arg

    if first_arg == second_arg:
        return True
    else:
        logger.error('''
        Assert Error happens:
        type of first_arg is {0}
        type of second_arg is {1}
        The first parameter is {2}
        The second parameter is {3}
        They're not the same!
        '''.format(type(first_arg), type(second_arg), first_arg, second_arg))
        logger.error(msg)
        return False

if __name__ == "__main__":
    pass
