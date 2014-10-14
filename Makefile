PROJECT = tirerl

DEPS = lager sync jiffy shotgun wpool
dep_lager = git https://github.com/basho/lager.git 2.0.3
dep_sync = git https://github.com/rustyio/sync.git HEAD
dep_jiffy = git https://github.com/davisp/jiffy.git 0.13.2
dep_shotgun = git https://github.com/inaka/shotgun.git 0.1.3
dep_wpool = git https://github.com/inaka/worker_pool 1.0

include erlang.mk

ERLC_OPTS += +'{parse_transform, lager_transform}'
TEST_ERLC_OPTS += +'{parse_transform, lager_transform}'

CT_SUITES = root_handler
CT_OPTS = -erl_args -config rel/sys.config

SHELL_OPTS = -name ${PROJECT}@`hostname` -s ${PROJECT} -s sync -config rel/sys.config
