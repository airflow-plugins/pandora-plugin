import getpass
import logging
import os

from builtins import bytes
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory


class SudoBashOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands but sudo's as another user to execute them.

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed.
    :type bash_command: string
    :param user: The user to run the command as.  The Airflow worker user
        must have permission to sudo as that user
    :type user: string
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior.
    :type env: dict
    :type output_encoding: output encoding of bash command
    """
    template_fields = ('bash_command', 'user', 'env', 'output_encoding')
    template_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            bash_command,
            user,
            xcom_push=False,
            env=None,
            output_encoding='utf-8',
            *args, **kwargs):

        super(SudoBashOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.user = user
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding

    def execute(self, context):
        """
        Execute the bash command in a temporary directory which will be cleaned afterwards.
        """
        logging.info("tmp dir root location: \n" + gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            os.chmod(tmp_dir, 0777)
            # Ensure the sudo user has perms to their current working directory for making tempfiles
            # This is not really a security flaw because the only thing in that dir is the
            # temp script, owned by the airflow user and any temp files made by the sudo user
            # and all of those will be created with the owning user's umask
            # If a process needs finer control over the tempfiles it creates, that process can chmod
            # them as they are created.
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                if self.user == getpass.getuser():  # don't try to sudo as yourself
                    f.write(bytes(self.bash_command, 'utf_8'))
                else:
                    sudo_cmd = "sudo -u {} sh -c '{}'".format(self.user, self.bash_command)
                    f.write(bytes(sudo_cmd, 'utf_8'))
                f.flush()

                logging.info('Temporary script location: {0}'.format(f.name))
                logging.info('Running command: {}'.format(self.bash_command))
                self.sp = Popen(['bash', f.name],  stdout=PIPE, stderr=STDOUT, cwd=tmp_dir, env=self.env)

                logging.info('Output:')
                line = ''
                for line in iter(self.sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).strip()
                    logging.info(line)
                self.sp.wait()
                logging.info("Command exited with return code {0}".format(self.sp.returncode))

                if self.sp.returncode:
                    raise AirflowException("Bash command failed")

        if self.xcom_push_flag:
            return line

    def on_kill(self):
        logging.warn('Sending SIGTERM signal to bash subprocess')
        self.sp.terminate()
