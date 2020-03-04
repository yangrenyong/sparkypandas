#
# MIT License
#
# Copyright (c) 2020 yangrenyong
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

"""
Some simple job preparation logics
"""

import __main__
from pyspark.sql import SparkSession


def start_spark(appName='sparkypandas-demo', master='local[*]', env='DEBUG'):
    is_repl = not (hasattr(__main__, '__file__'))
    is_debug = 'DEBUG' == env

    if not (is_repl or is_debug):
        session_builder = (
            SparkSession
                .builder
                .enableHiveSupport()
                .appName(appName))
    else:
        session_builder = (
            SparkSession
                .builder
                .master(master)
                .enableHiveSupport()
                .appName(appName))

    return session_builder.getOrCreate()