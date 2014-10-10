-module(tirerl_sup).

-behaviour(supervisor).

%% API
-export([
         start_link/0,
         start_pool/2,
         stop_pool/1
        ]).

%% Supervisor callbacks
-export([init/1]).

-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.

-spec start_link() -> startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init(Args :: term()) -> {ok, term()}.
init([]) ->
    {ok, {{one_for_one, 1000, 3600}, []}}.

%% Worker pool
-spec start_pool(tirerl:pool_name(), list()) -> {ok, pid()}.
start_pool(Name, Opts) ->
    WPoolOptions  = [{overrun_warning, infinity},
                     {overrun_handler, {error_logger, warning_report}},
                     {workers, 50},
                     {worker, {tirerl, {Name, Opts}}}
                    ],

    PoolSpec = {Name,
                {wpool, start_pool, [Name, WPoolOptions ++ Opts]},
                permanent,
                5000,
                supervisor,
                [wpool]
               },

    supervisor:start_child(?MODULE, PoolSpec).

-spec stop_pool(tirerl:pool_name()) -> ok.
stop_pool(Name) ->
    ok = supervisor:terminate_child(?MODULE, Name).
