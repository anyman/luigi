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

import os
import tempfile
import logging
import warnings
import sys
import random
import tarfile

from luigi import mrrunner
import luigi.configuration
import luigi.hadoop
import pickle

logger = logging.getLogger('luigi-interface')


class YarnTask(luigi.Task):
    def __init__(self):
        self.python_yarn_jar = 'distshell.jar'

    def _dump(self, dir=''):
        """Dump instance to file."""
        file_name = os.path.join(dir, 'job-instance.pickle')
        if self.__module__ == '__main__':
            d = pickle.dumps(self)
            module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
            d = d.replace('(c__main__', "(c" + module_name)
            open(file_name, "w").write(d)

        else:
            pickle.dump(self, open(file_name, "w"))

    def _build_tar(self, archive_path, files):
        # find the path to out runner.py
        runner_path = mrrunner.__file__
        # assume source is next to compiled
        if runner_path.endswith("pyc"):
            runner_path = runner_path[:-3] + "py"

        archive = tarfile.open(archive_path, 'w')
        archive.add(runner_path, os.path.basename(runner_path))
        for f in files:
            archive.add(f, os.path.basename(f))
        archive.close()
        return archive

    def _setup_remote(self):
        pass

    def run(self):
        job = self
        packages = [luigi] + list(luigi.hadoop._attached_packages)

        # find the module containing the job
        packages.append(__import__(job.__module__, None, None, 'dummy'))

        base_tmp_dir = luigi.configuration.get_config().get('core', 'tmp-dir', None)
        if base_tmp_dir:
            warnings.warn("The core.tmp-dir configuration item is"\
                          " deprecated, please use the TMPDIR"\
                          " environment variable if you wish"\
                          " to control where luigi.hadoop may"\
                          " create temporary files and directories.")
            self.tmp_dir = os.path.join(base_tmp_dir, 'hadoop_job_%016x' % random.getrandbits(64))
            os.makedirs(self.tmp_dir)
        else:
            self.tmp_dir = tempfile.mkdtemp()

        # build arguments
        config = luigi.configuration.get_config()
        python_executable = config.get('hadoop', 'python-executable', 'python')
        command = '{0} mrrunner.py yarn'.format(python_executable)

        # replace output with a temporary work directory
#        output_final = job.output().path
#        output_tmp_fn = output_final + '-temp-' + datetime.datetime.now().isoformat().replace(':', '-')
#        tmp_target = luigi.hdfs.HdfsTarget(output_tmp_fn, is_tmp=True)

        arglist = ['yarn', 'jar', self.python_yarn_jar, '-jar', self.python_yarn_jar]
        luigi.hadoop.create_packages_archive(packages, self.tmp_dir + '/packages.tar')
        job._dump(self.tmp_dir)

        files = [self.tmp_dir + '/packages.tar', self.tmp_dir + '/job-instance.pickle']
        archive_path = os.path.join(self.tmp_dir, 'archive.tar')
        self._build_tar(archive_path, files)
        arglist.extend(['-archive', archive_path])
        arglist.extend(['-shell_command', command])

        print arglist
        luigi.hadoop.run_and_track_hadoop_job(arglist)

        # rename temporary work directory to given output
#        tmp_target.move(output_final, raise_if_exists=True)
#        self.finish()
