# Copyright (c) 2014 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import unittest
import os
import tempfile
import tarfile

from luigi.hdfs import HdfsTarget
import luigi.yarn


class SimpleTask(luigi.yarn.YarnTask):
    def output(self):
        return HdfsTarget('/user/nyman/yarntest')

    def main(self):
        f = self.output().open('w')
        f.write('yarn in luigi\n')
        f.close()


class YarnTest(unittest.TestCase):

    def testBuildArchive(self):
        task = SimpleTask()
        path = os.path.join(tempfile.mkdtemp(), 'archive.tar')
        task._build_tar(path, [])
        archive = tarfile.open(path, 'r')
        self.assertTrue('mrrunner.py' in archive.getnames())
        archive.close()

    def testRunTask(self):
        task = SimpleTask()
        task.run()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()