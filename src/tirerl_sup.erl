-module(tirerl_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%% Helper macro for declaring children of supervisor
-define(SUPERVISOR(Id, Module, Args), {Id, {Module, start_link, Args}, permanent, 5000, supervisor, [Module]}).
-define(WORKER(Id, Module, Args), {Id, {Module, start_link, Args}, permanent, 5000, worker, [Module]}).

-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.

-spec start_link() -> startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init(Args :: term()) ->
    {ok, {{RestartStrategy :: supervisor:strategy(),
           MaxR :: non_neg_integer(),
           MaxT :: non_neg_integer()},
          [ChildSpec :: supervisor:child_spec()]}}.
init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    % Start up user and department first, 'cos the cache depends on these being up
    {ok, {SupFlags,
          [?SUPERVISOR(tirerl_poolboy_sup, tirerl_poolboy_sup, [])]}}.
