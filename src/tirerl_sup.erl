-module(tirerl_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.

-spec start_link() -> startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init(Args :: term()) ->
    {ok, {{supervisor:strategy(),
           non_neg_integer(),
           non_neg_integer()},
          [supervisor:child_spec()]}}.
init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    PoolSupSpec = {tirerl_poolboy_sup,
                   {tirerl_poolboy_sup, start_link, []},
                   permanent,
                   5000,
                   supervisor,
                   [tirerl_poolboy_sup]
                  },

    {ok,
     {
       SupFlags,
       [PoolSupSpec]
     }
    }.
