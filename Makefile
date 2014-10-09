PROJECT = tirerl

DEPS = lager sync
dep_lager = git https://github.com/basho/lager.git master
dep_sync = git https://github.com/rustyio/sync.git master

include erlang.mk

ERLC_OPTS += +'{parse_transform, lager_transform}'
TEST_ERLC_OPTS += +'{parse_transform, lager_transform}'

CT_SUITES = root_handler
CT_OPTS = -erl_args -config rel/sys.config

SHELL_OPTS = -name ${PROJECT}@`hostname` -s ${PROJECT} -config rel/sys.config
