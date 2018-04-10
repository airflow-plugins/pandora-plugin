import logging
import os

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults

from subprocess import check_output, CalledProcessError


class JavaOperator(BaseOperator):
    """
    :param maven_coordinate: Metadata of the jar in Maven you want to download
    :type maven_coordinate: list
    :param repositories: Where the jar is located in Maven.
    :type repositories: list
    :param main_class: The location of user-defined main class you want to execute
    :type main_class: string
    :type op_args: list
    :param op_args: a list of positional arguments that will get unpacked in the order you provided
        when executing your jar
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked in an arbitary but deterministic order
        when executing your jar after the positional arguments
    :type op_kwargs: dict
    :param fetch_mode: Mode to use when downloading the jars
        By default, it will fetch things missing from cache.
        Here is a list of modes available <offline|update-changing|update|missing|force>
    :type fetch_mode: string
    :param cache_directory: The location of where the jars are cached.
        By default, they are located at your user folder under '.coursier/cache/v1'
    :type cache_directory: string
    :param extra_coursier_params: a list of strings that can be args or kwargs
    :type extra_coursier_params: list
    """
    template_fields = ('main_class', 'repositories', 'op_args', 'op_kwargs',
                       'fetch_mode', 'cache_directory', 'extra_coursier_params')
    ui_color = '#F5C957'

    @apply_defaults
    def __init__(self,
                 maven_coordinates,
                 repositories=None,
                 main_class=None,
                 op_args=None,
                 op_kwargs=None,
                 fetch_mode='missing',
                 cache_directory=os.path.join(os.path.expanduser('~'), '.coursier/cache/v1'),
                 extra_coursier_params=None,
                 *args, **kwargs):
        super(JavaOperator, self).__init__(*args, **kwargs)

        self.maven_coordinates = maven_coordinates
        self.repositories = repositories
        self.main_class = main_class
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.fetch_mode = fetch_mode
        self.cache_directory = cache_directory
        self.extra_coursier_params = extra_coursier_params or []

    def run_coursier(self):
        """
        Builds a bash command to download all transitive dependencies of a maven coordinate.
        It can return java -cp compatible output for executing.

        This is done through coursier. Find more information at: https://github.com/coursier/coursier.
        """
        cmd = ['coursier', 'fetch']

        cmd.extend(self.extra_coursier_params)
        cmd.extend(['--mode', self.fetch_mode])
        cmd.extend(['--cache', self.cache_directory])

        if self.main_class:
            cmd.extend(['--classpath'])

        for repo in self.repositories:
            cmd.extend(['--repository', repo])
        for coordinate in self.maven_coordinates:
            if not isinstance(coordinate, MavenCoordinate):
                raise AirflowException('Please use the MavenCoordinate class. Current type: {0}, current value: {1}'
                                       .format(type(coordinate), coordinate))
            cmd.extend([coordinate.get_coordinate(), '--artifact-type', coordinate.packaging])

        logging.info('Executing %s', cmd)
        try:
            return check_output(cmd)
        except CalledProcessError:
            raise AirflowException("Failed to fetch requested maven coordinates")

    def execute(self, context):
        """
        Runs the coursier command.

        Returns jvm exit code instead of main's exit code. When an exception is
        caught, the exit code returned will be 0 meaning a false positive result.
        It is best to include `System.exit()` with some non-zero value when an exception happens
        if that is the intended flow.
        """
        output = self.run_coursier()
        if self.main_class:
            cmd = ['java', '-cp', '"'+output+'"', self.main_class]
            cmd.extend(self.op_args)
            for k, v in self.op_kwargs.items():
                cmd.extend([k, v])

            bash_command = ' '.join(cmd)
            BashOperator(bash_command=bash_command, task_id='inner_bash').execute(self)
        else:
            logging.info(output)


class MavenCoordinate:
    """
    For accuracy, copy/paste information direct from Nexus.
    Generally, leave packaging as default.

    Find more information here: https://maven.apache.org/pom.html#Maven_Coordinates
    :param group_id: Unique identification amongst an organization or a project
    :type group_id: string
    :param artifact_id: The name that the project is known by
    :type artifact_id: string
    :param version: Version of project
    :type version: string
    :param packaging: Type of project
    :type packaging: string
    """
    def __init__(self,
                 group_id,
                 artifact_id,
                 version,
                 packaging='jar,bundle'
                 ):
        self.group_id = group_id
        self.artifact_id = artifact_id
        self.version = version
        self.packaging = packaging

    def __repr__(self):
        return self.get_coordinate()

    def get_coordinate(self):
        return ':'.join([self.group_id, self.artifact_id, self.version])
