import os.path
import pprint
import copy
import hashlib
import re
from voluptuous import Required, Invalid

import dbt.deprecations
import dbt.contracts.connection
import dbt.clients.yaml_helper
import dbt.clients.jinja
import dbt.compat
import dbt.context.common
import dbt.clients.system
import dbt.ui.printer
import dbt.links

from dbt.logger import GLOBAL_LOGGER as logger  # noqa

default_project_cfg = {
    'source-paths': ['models'],
    'macro-paths': ['macros'],
    'data-paths': ['data'],
    'test-paths': ['test'],
    'target-path': 'target',
    'log-path': 'logs',
    'clean-targets': ['target'],
    'outputs': {'default': {}},
    'target': 'default',
    'models': {},
    'quoting': {},
    'profile': None,
    'packages': [],
    'modules-path': 'dbt_modules'
}

default_profiles = {}

default_profiles_dir = os.path.join(os.path.expanduser('~'), '.dbt')

NO_SUPPLIED_PROFILE_ERROR = """\
dbt cannot run because no profile was specified for this dbt project.
To specify a profile for this project, add a line like the this to
your dbt_project.yml file:

profile: [profile name]

Here, [profile name] should be replaced with a profile name
defined in your profiles.yml file. You can find profiles.yml here:

{profiles_file}/profiles.yml
""".format(profiles_file=default_profiles_dir)


class DbtProjectError(Exception):
    def __init__(self, message, project):
        self.project = project
        super(DbtProjectError, self).__init__(message)


class DbtProfileError(Exception):
    def __init__(self, message, project):
        super(DbtProfileError, self).__init__(message)


class Project(object):

    def __init__(self, cfg, profiles, profiles_dir, profile_to_load=None,
                 args=None):

        self.cfg = default_project_cfg.copy()
        self.cfg.update(cfg)
        self.profiles = default_profiles.copy()
        self.profiles.update(profiles)
        self.profiles_dir = profiles_dir
        self.profile_to_load = profile_to_load
        self.args = args

        # load profile from dbt_config.yml if cli arg isn't supplied
        if self.profile_to_load is None and self.cfg['profile'] is not None:
            self.profile_to_load = self.cfg['profile']

        if self.profile_to_load is None:
            raise DbtProjectError(NO_SUPPLIED_PROFILE_ERROR, self)

        if self.profile_to_load in self.profiles:
            self.cfg.update(self.profiles[self.profile_to_load])
            self.compile_and_update_target()

        else:
            raise DbtProjectError(
                "Could not find profile named '{}'"
                .format(self.profile_to_load), self)

        if self.cfg.get('models') is None:
            self.cfg['models'] = {}

        if self.cfg.get('quoting') is None:
            self.cfg['quoting'] = {}

        if self.cfg['models'].get('vars') is None:
            self.cfg['models']['vars'] = {}

        if self.cfg.get('log-path') is not None:
            self.cfg['log-path'] = dbt.clients.system.resolve_path_from_base(
                self.cfg.get('log-path'),
                self.cfg.get('project-root'))

        if self.cfg.get('target-path') is not None:
            self.cfg['target-path'] = dbt.clients.system.resolve_path_from_base(
                self.cfg.get('target-path'),
                self.cfg.get('project-root'))

        global_vars = dbt.utils.parse_cli_vars(getattr(args, 'vars', '{}'))
        self.cfg['cli_vars'] = global_vars

    def __str__(self):
        return pprint.pformat({'project': self.cfg, 'profiles': self.profiles})

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        return self.cfg.__getitem__(key)

    def __contains__(self, key):
        return self.cfg.__contains__(key)

    def __setitem__(self, key, value):
        return self.cfg.__setitem__(key, value)

    def get(self, key, default=None):
        return self.cfg.get(key, default)

    def handle_deprecations(self):
        pass

    def is_valid_package_name(self):
        if re.match(r"^[^\d\W]\w*\Z", self['name']):
            return True
        else:
            return False

    def compile_target(self, target_cfg):
        ctx = self.base_context()

        compiled = {}
        for (key, value) in target_cfg.items():
            is_str = isinstance(value, dbt.compat.basestring)

            if is_str:
                compiled_val = dbt.clients.jinja.get_rendered(value, ctx)
            else:
                compiled_val = value

            compiled[key] = compiled_val

        return compiled

    def compile_and_update_target(self):
        target = self.cfg['target']
        self.cfg['outputs'][target].update(self.run_environment())

    def run_environment(self):
        target_name = self.cfg['target']
        if target_name in self.cfg['outputs']:
            target_cfg = self.cfg['outputs'][target_name]
            return self.compile_target(target_cfg)
        else:
            raise DbtProfileError(
                    "'target' config was not found in profile entry for "
                    "'{}'".format(target_name), self)

    def get_target(self):
        ctx = self.context().get('env').copy()
        ctx['name'] = self.cfg['target']
        return ctx

    def base_context(self):
        return {
            'env_var': dbt.context.common._env_var
        }

    def context(self):
        target_cfg = self.run_environment()
        filtered_target = copy.deepcopy(target_cfg)
        filtered_target.pop('pass', None)

        ctx = self.base_context()
        ctx.update({
            'env': filtered_target
        })

        return ctx

    def validate(self):
        self.handle_deprecations()

        target_cfg = self.run_environment()
        package_name = self.cfg.get('name', None)
        package_version = self.cfg.get('version', None)

        if package_name is None or package_version is None:
            raise DbtProjectError(
                "Project name and version is not provided", self)

        if not self.is_valid_package_name():
            raise DbtProjectError(
                ('Package name can only contain letters, numbers, and '
                 'underscores, and must start with a letter.'), self)

        db_type = target_cfg.get('type')
        validator = dbt.contracts.connection.credentials_mapping.get(db_type)

        if validator is None:
            valid_types = dbt.contracts.connection.credentials_mapping.keys()
            raise DbtProjectError(
                "Invalid db type '{}' should be one of [{}]".format(
                    db_type,
                    ", ".join(valid_types)), self)

        validator = validator.extend({
            Required('type'): dbt.compat.basestring,
            Required('threads'): int,
        })

        try:
            validator(target_cfg)
        except Invalid as e:
            if 'extra keys not allowed' in str(e):
                raise DbtProjectError(
                    "Extra project configuration '{}' is not recognized"
                    .format('.'.join(e.path)), self)
            else:
                # TODO : does this fail if eg. project is missing?
                raise DbtProjectError(
                    "Expected project configuration '{}' was not supplied"
                    .format('.'.join(e.path)), self)

    def log_warnings(self):
        target_cfg = self.run_environment()
        db_type = target_cfg.get('type')

        if db_type == 'snowflake' and self.cfg \
                                          .get('quoting', {}) \
                                          .get('identifier') is None:
            msg = dbt.ui.printer.yellow(
                'You are using Snowflake, but you did not specify a '
                'quoting strategy for your identifiers.\nQuoting '
                'behavior for Snowflake will change in a future release, '
                'so it is recommended that you define this explicitly.\n\n'
                'For more information, see: {}\n'
            )

            logger.warn(msg.format(dbt.links.SnowflakeQuotingDocs))

    def hashed_name(self):
        if self.cfg.get("name", None) is None:
            return None

        project_name = self['name']
        return hashlib.md5(project_name.encode('utf-8')).hexdigest()


def read_profiles(profiles_dir=None):
    if profiles_dir is None:
        profiles_dir = default_profiles_dir

    raw_profiles = dbt.config.read_profile(profiles_dir)

    if raw_profiles is None:
        profiles = {}
    else:
        profiles = {k: v for (k, v) in raw_profiles.items() if k != 'config'}

    return profiles


def read_packages(project_dir):

    package_filepath = dbt.clients.system.resolve_path_from_base(
            'packages.yml', project_dir)

    if dbt.clients.system.path_exists(package_filepath):
        package_file_contents = dbt.clients.system.load_file_contents(
                package_filepath)
        package_cfg = dbt.clients.yaml_helper.load_yaml_text(
                package_file_contents)
    else:
        package_cfg = {}

    return package_cfg.get('packages', [])


def read_project(project_filepath, profiles_dir=None, validate=True,
                 profile_to_load=None, args=None):
    if profiles_dir is None:
        profiles_dir = default_profiles_dir

    project_dir = os.path.dirname(os.path.abspath(project_filepath))
    project_file_contents = dbt.clients.system.load_file_contents(
            project_filepath)

    project_cfg = dbt.clients.yaml_helper.load_yaml_text(project_file_contents)
    package_cfg = read_packages(project_dir)

    project_cfg['project-root'] = project_dir
    project_cfg['packages'] = package_cfg

    profiles = read_profiles(profiles_dir)
    proj = Project(project_cfg,
                   profiles,
                   profiles_dir,
                   profile_to_load=profile_to_load,
                   args=args)

    if validate:
        proj.validate()

    return proj
