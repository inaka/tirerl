%% @doc Main module with all API functions.
-module(tirerl).

-behaviour(application).

-include("tirerl.hrl").

%% Application callbacks
-export([
         start/0,
         start/2,
         stop/0,
         stop/1
        ]).

-export([stop_pool/1]).
-export([start_pool/1, start_pool/2]).

%% ElasticSearch
% Tests
-export([is_index/2]).
-export([is_type/3]).
-export([is_doc/4]).

% Cluster helpers
-export([health/1]).
-export([cluster_state/1, cluster_state/2]).
-export([state/1, state/2]).
-export([nodes_info/1, nodes_info/2, nodes_info/3]).
-export([nodes_stats/1, nodes_stats/2, nodes_stats/3]).

% Index CRUD
-export([create_index/2, create_index/3]).
-export([delete_index/1, delete_index/2]).
-export([open_index/2]).
-export([close_index/2]).

% Doc CRUD
-export([insert_doc/5, insert_doc/6]).
-export([update_doc/5, update_doc/6]).
-export([get_doc/4, get_doc/5]).
-export([mget_doc/2, mget_doc/3, mget_doc/4]).
-export([delete_doc/4, delete_doc/5]).
-export([search/4, search/5]).
-export([count/2, count/3, count/4, count/5]).
-export([delete_by_query/2,
         delete_by_query/3,
         delete_by_query/4,
         delete_by_query/5]).
-export([bulk/2, bulk/3, bulk/4]).

%% Index helpers
-export([status/2]).
-export([indices_stats/2]).
-export([refresh/1, refresh/2]).
-export([flush/1, flush/2]).
-export([optimize/1, optimize/2]).
-export([clear_cache/1, clear_cache/2, clear_cache/3]).
-export([segments/1, segments/2]).

% Mapping CRUD
-export([put_mapping/4]).
-export([get_mapping/3]).
-export([delete_mapping/3]).

% ALIASES CRUD
-export([aliases/2]).
-export([insert_alias/3, insert_alias/4]).
-export([delete_alias/3]).
-export([is_alias/3]).
-export([get_alias/3]).


%% Types

-type node_name()   :: binary().
-type index()       :: binary().
-type type()        :: binary().
-type id()          :: binary() | undefined.
-type doc()         :: binary() | map().

-type pool_name()   :: atom().
-type params()      :: [tuple()].
-type destination() :: atom() | pid().
-type response()    :: tirerl_worker:response().

-export_type([pool_name/0, destination/0, params/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Application Behavior
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Start the application and all its dependencies.
-spec start() -> {ok, [atom()]}.
start() ->
    application:ensure_all_started(tirerl).

%% @doc Stop the application and all its dependencies.
-spec stop() -> ok.
stop() ->
    application:stop(tirerl).

-spec start(StartType :: normal | {takeover, node()} | {failover, node()},
            StartArgs :: [term()]) ->
    {ok, pid()}.
start(_StartType, _StartArgs) ->
    tirerl_sup:start_link().

-spec stop([]) -> ok.
stop(_State) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Worker Pool API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Start a poolboy instance
-spec start_pool(pool_name()) -> supervisor:startchild_ret().
start_pool(Name) ->
    start_pool(Name, []).

%% @doc Start a poolboy instance
-spec start_pool(pool_name(), params()) -> supervisor:startchild_ret().
start_pool(PoolName, Options) when is_list(Options) ->
    tirerl_sup:start_pool(PoolName, Options).

%% @doc Stop a worker pool instance
-spec stop_pool(pool_name()) -> ok | tirerl_worker:error().
stop_pool(Name) ->
    wpool:stop_pool(Name).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ElasticSearch API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Get the health the  ElasticSearch cluster
-spec health(destination()) -> response().
health(Destination) ->
    route_call(Destination, {health}, infinity).

%% @equiv cluster_state(Destination, [])
-spec cluster_state(destination()) -> response().
cluster_state(Destination) ->
    cluster_state(Destination, []).

%% @doc Get the state of the  ElasticSearch cluster
-spec cluster_state(destination(), params()) -> response().
%% @equiv state(Destination, Params)
cluster_state(Destination, Params) when is_list(Params) ->
    state(Destination, Params).

%% @equiv state(Destination, [])
-spec state(destination()) -> response().
state(Destination) ->
    state(Destination, []).

%% @doc Get the state of the  ElasticSearch cluster
-spec state(destination(), params()) -> response().
state(Destination, Params) when is_list(Params) ->
    route_call(Destination, {state, Params}, infinity).

%% @equiv nodes_info(Destination, [], [])
-spec nodes_info(destination()) -> response().
nodes_info(Destination) ->
    nodes_info(Destination, [], []).

%% @equiv nodes_info(Destination, [NodeName], [])
-spec nodes_info(destination(), node_name()) -> response().
nodes_info(Destination, NodeName) when is_binary(NodeName) ->
    nodes_info(Destination, [NodeName], []);
%% @equiv nodes_info(Destination, NodeNames, [])
nodes_info(Destination, NodeNames) when is_list(NodeNames) ->
    nodes_info(Destination, NodeNames, []).

%% @doc Get the nodes_info of the  ElasticSearch cluster
-spec nodes_info(destination(), [node_name()], params()) -> response().
nodes_info(Destination, NodeNames, Params)
  when is_list(NodeNames), is_list(Params) ->
    route_call(Destination, {nodes_info, NodeNames, Params}, infinity).

%% @equiv nodes_stats(Destination, [], [])
-spec nodes_stats(destination()) -> response().
nodes_stats(Destination) ->
    nodes_stats(Destination, [], []).

%% @equiv nodes_stats(Destination, [NodeName], [])
-spec nodes_stats(destination(), node_name()) -> response().
nodes_stats(Destination, NodeName) when is_binary(NodeName) ->
    nodes_stats(Destination, [NodeName], []);
%% @equiv nodes_stats(Destination, NodeNames, [])
nodes_stats(Destination, NodeNames) when is_list(NodeNames) ->
    nodes_stats(Destination, NodeNames, []).

%% @doc Get the nodes_stats of the  ElasticSearch cluster
-spec nodes_stats(destination(), [node_name()], params()) -> response().
nodes_stats(Destination, NodeNames, Params)
  when is_list(NodeNames), is_list(Params) ->
    route_call(Destination, {nodes_stats, NodeNames, Params}, infinity).

%% @doc Get the status of an index/indices in the  ElasticSearch cluster
-spec status(destination(), index() | [index()]) -> response().
status(Destination, Index) when is_binary(Index) ->
    status(Destination, [Index]);
status(Destination, Indexes) when is_list(Indexes)->
    route_call(Destination, {status, Indexes}, infinity).

%% @doc Get the stats of an index/indices in the  ElasticSearch cluster
-spec indices_stats(destination(), index() | [index()]) -> response().
indices_stats(Destination, Index) when is_binary(Index) ->
    indices_stats(Destination, [Index]);
indices_stats(Destination, Indexes) when is_list(Indexes)->
    route_call(Destination, {indices_stats, Indexes}, infinity).

%% @equiv create_index(Destination, Index, <<>>)
-spec create_index(destination(), index()) -> response().
create_index(Destination, Index) when is_binary(Index) ->
    create_index(Destination, Index, <<>>).

%% @doc Create an index in the ElasticSearch cluster
-spec create_index(destination(), index(), doc()) -> response().
create_index(Destination, Index, Doc)
  when is_binary(Index) andalso (is_binary(Doc) orelse is_map(Doc)) ->
    route_call(Destination, {create_index, Index, Doc}, infinity).

%% @doc Delete all the indices in the ElasticSearch cluster
-spec delete_index(destination()) -> response().
delete_index(Destination) ->
    delete_index(Destination, ?ALL).

%% @doc Delete an index(es) in the ElasticSearch cluster
-spec delete_index(destination(), index() | [index()]) -> response().
delete_index(Destination, Index) when is_binary(Index) ->
    delete_index(Destination, [Index]);
delete_index(Destination, Index) when is_list(Index) ->
    route_call(Destination, {delete_index, Index}, infinity).

%% @doc Open an index in the ElasticSearch cluster
-spec open_index(destination(), index()) -> response().
open_index(Destination, Index) when is_binary(Index) ->
    route_call(Destination, {open_index, Index}, infinity).

%% @doc Close an index in the ElasticSearch cluster
-spec close_index(destination(), index()) -> response().
close_index(Destination, Index) when is_binary(Index) ->
    route_call(Destination, {close_index, Index}, infinity).

%% @doc Check if an index/indices exists in the ElasticSearch cluster
-spec is_index(destination(), index() | [index()]) -> boolean().
is_index(Destination, Index) when is_binary(Index) ->
    is_index(Destination, [Index]);
is_index(Destination, Indexes) when is_list(Indexes) ->
    Result = route_call(Destination, {is_index, Indexes}, infinity),
    boolean_result(Result).

%% @equiv count(Destination, <<"_all">>, [], Doc, [])
-spec count(destination(), doc()) -> response().
count(Destination, Doc) when (is_binary(Doc) orelse is_map(Doc)) ->
    count(Destination, ?ALL, [], Doc, []).

%% @equiv count(Destination, <<"_all">>, [], Doc, Params)
-spec count(destination(), doc(), params()) -> response().
count(Destination, Doc, Params)
  when (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    count(Destination, ?ALL, [], Doc, Params).

%% @equiv count(Destination, Index, [], Doc, Params)
-spec count(destination(), index() | [index()], doc(), params()) -> response().
count(Destination, Index, Doc, Params)
  when is_binary(Index)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    count(Destination, [Index], [], Doc, Params);
count(Destination, Indexes, Doc, Params)
  when is_list(Indexes)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    count(Destination, Indexes, [], Doc, Params).

%% @doc Get the number of matches for a query
-spec count(destination(),
            index() | [index()],
            type() | [type()],
            doc(),
            params()) -> response().
count(Destination, Index, Type, Doc, Params)
  when is_binary(Index)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    count(Destination, [Index], [Type], Doc, Params);
count(Destination, Indexes, Type, Doc, Params)
  when is_list(Indexes)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    count(Destination, Indexes, [Type], Doc, Params);
count(Destination, Index, Types, Doc, Params)
  when is_binary(Index)
       andalso is_list(Types)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    count(Destination, [Index], Types, Doc, Params);
count(Destination, Indexes, Types, Doc, Params)
  when is_list(Indexes)
       andalso is_list(Types)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    route_call(Destination, {count, Indexes, Types, Doc, Params}, infinity).

%% @equiv delete_by_query(Destination, <<"_all">>, [], Doc, [])
-spec delete_by_query(destination(), doc()) -> response().
delete_by_query(Destination, Doc) when (is_binary(Doc) orelse is_map(Doc)) ->
    delete_by_query(Destination, ?ALL, [], Doc, []).

%% @equiv delete_by_query(Destination, <<"_all">>, [], Doc, Params)
-spec delete_by_query(destination(), doc(), params()) -> response().
delete_by_query(Destination, Doc, Params)
  when (is_binary(Doc) orelse is_map(Doc)),
       is_list(Params) ->
    delete_by_query(Destination, ?ALL, [], Doc, Params).

%% @equiv delete_by_query(Destination, Index, [], Doc, Params)
-spec delete_by_query(destination(),
                      index() | [index()],
                      doc(),
                      params()) -> response().
delete_by_query(Destination, Index, Doc, Params)
  when is_binary(Index)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    delete_by_query(Destination, [Index], [], Doc, Params);
delete_by_query(Destination, Indexes, Doc, Params)
  when is_list(Indexes)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    delete_by_query(Destination, Indexes, [], Doc, Params).

%% @doc Get the number of matches for a query
-spec delete_by_query(destination(),
                      index() | [index()],
                      type() | [type()],
                      doc(),
                      params()) -> response().
delete_by_query(Destination, Index, Type, Doc, Params)
  when is_binary(Index)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    delete_by_query(Destination, [Index], [Type], Doc, Params);
delete_by_query(Destination, Indexes, Type, Doc, Params)
  when is_list(Indexes)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    delete_by_query(Destination, Indexes, [Type], Doc, Params);
delete_by_query(Destination, Index, Types, Doc, Params)
  when is_binary(Index)
       andalso is_list(Types)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    delete_by_query(Destination, [Index], Types, Doc, Params);
delete_by_query(Destination, Indexes, Types, Doc, Params)
  when is_list(Indexes)
       andalso is_list(Types)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    route_call(Destination,
               {delete_by_query, Indexes, Types, Doc, Params},
               infinity).

%% @doc Check if a type exists in an index/indices in the ElasticSearch cluster
-spec is_type(destination(), index() | [index()], type() | [type()]) ->
    boolean().
is_type(Destination, Index, Type) when is_binary(Index), is_binary(Type) ->
    is_type(Destination, [Index], [Type]);
is_type(Destination, Indexes, Type) when is_list(Indexes), is_binary(Type) ->
    is_type(Destination, Indexes, [Type]);
is_type(Destination, Index, Types) when is_binary(Index), is_list(Types) ->
    is_type(Destination, [Index], Types);
is_type(Destination, Indexes, Types) when is_list(Indexes), is_list(Types) ->
    Result = route_call(Destination, {is_type, Indexes, Types}, infinity),
    boolean_result(Result).

%% @equiv insert_doc(Destination, Index, Type, Id, Doc, [])
-spec insert_doc(destination(), index(), type(), id(), doc()) -> response().
insert_doc(Destination, Index, Type, Id, Doc)
  when is_binary(Index)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc)) ->
    insert_doc(Destination, Index, Type, Id, Doc, []).

%% @doc Insert a doc into the ElasticSearch cluster
-spec insert_doc(destination(), index(), type(), id(), doc(), params()) ->
    response().
insert_doc(Destination, Index, Type, Id, Doc, Params)
  when is_binary(Index)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    route_call(Destination,
               {insert_doc, Index, Type, Id, Doc, Params},
               infinity).

%% @equiv update_doc(Destination, Index, Type, Id, Doc, [])
-spec update_doc(destination(), index(), type(), id(), doc()) -> response().
update_doc(Destination, Index, Type, Id, Doc)
  when is_binary(Index)
       andalso is_binary(Type)
       andalso  is_binary(Id)
       andalso (is_binary(Doc) orelse is_map(Doc)) ->
    update_doc(Destination, Index, Type, Id, Doc, []).

%% @doc Insert a doc into the ElasticSearch cluster
-spec update_doc(destination(), index(), type(), id(), doc(), params()) ->
    response().
update_doc(Destination, Index, Type, Id, Doc, Params)
  when is_binary(Index)
       andalso is_binary(Type)
       andalso is_binary(Id)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    route_call(Destination,
               {update_doc, Index, Type, Id, Doc, Params},
               infinity).

%% @doc Checks to see if the doc exists
-spec is_doc(destination(), index(), type(), id()) -> boolean().
is_doc(Destination, Index, Type, Id)
  when is_binary(Index), is_binary(Type), is_binary(Id) ->
    Result = route_call(Destination, {is_doc, Index, Type, Id}, infinity),
    boolean_result(Result).

%% @equiv get_doc(Destination, Index, Type, Id, [])
-spec get_doc(destination(), index(), type(), id()) -> response().
get_doc(Destination, Index, Type, Id)
  when is_binary(Index), is_binary(Type) ->
    get_doc(Destination, Index, Type, Id, []).

%% @doc Get a doc from the ElasticSearch cluster
-spec get_doc(destination(), index(), type(), id(), params()) -> response().
get_doc(Destination, Index, Type, Id, Params)
  when is_binary(Index),
       is_binary(Type),
       is_binary(Id),
       is_list(Params) ->
    route_call(Destination, {get_doc, Index, Type, Id, Params}, infinity).

%% @equiv mget_doc(Destination, <<>>, <<>>, Doc)
-spec mget_doc(destination(), doc()) -> response().
mget_doc(Destination, Doc) when (is_binary(Doc) orelse is_map(Doc)) ->
    mget_doc(Destination, <<>>, <<>>, Doc).

%% @equiv mget_doc(Destination, Index, <<>>, Doc)
-spec mget_doc(destination(), index(), doc()) -> response().
mget_doc(Destination, Index, Doc)
  when is_binary(Index)
       andalso (is_binary(Doc) orelse is_map(Doc))->
    mget_doc(Destination, Index, <<>>, Doc).

%% @doc Get a doc from the ElasticSearch cluster
-spec mget_doc(destination(), index(), type(), doc()) -> response().
mget_doc(Destination, Index, Type, Doc)
  when is_binary(Index)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc))->
    route_call(Destination, {mget_doc, Index, Type, Doc}, infinity).

%% @equiv delete_doc(Destination, Index, Type, Id, [])
-spec delete_doc(destination(), index(), type(), id()) -> response().
delete_doc(Destination, Index, Type, Id)
  when is_binary(Index), is_binary(Type) ->
    delete_doc(Destination, Index, Type, Id, []).
%% @doc Delete a doc from the ElasticSearch cluster
-spec delete_doc(destination(), index(), type(), id(), params()) -> response().
delete_doc(Destination, Index, Type, Id, Params)
  when is_binary(Index), is_binary(Type), is_binary(Id), is_list(Params)->
    route_call(Destination, {delete_doc, Index, Type, Id, Params}, infinity).

%% @equiv search(Destination, Index, Type, Doc, [])
-spec search(destination(), index(), type(), doc()) -> response().
search(Destination, Index, Type, Doc)
  when is_binary(Index)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc))->
    search(Destination, Index, Type, Doc, []).
%% @doc Search for docs in the ElasticSearch cluster
-spec search(destination(), index(), type(), doc(), params()) -> response().
search(Destination, Index, Type, Doc, Params)
  when is_binary(Index)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc))
       andalso is_list(Params) ->
    route_call(Destination, {search, Index, Type, Doc, Params}, infinity).

%% @doc Perform bulk operations on the ElasticSearch cluster
-spec bulk(destination(), doc()) -> response().
bulk(Destination, Doc) when (is_binary(Doc) orelse is_map(Doc)) ->
    bulk(Destination, <<>>, <<>>, Doc).

%% @doc Perform bulk operations on the ElasticSearch cluster
%% with a default index
-spec bulk(destination(), index(), doc()) -> response().
bulk(Destination, Index, Doc)
  when is_binary(Index)
       andalso (is_binary(Doc) orelse is_map(Doc)) ->
    bulk(Destination, Index, <<>>, Doc).

%% @doc Perform bulk operations on the ElasticSearch cluster
%% with a default index and a default type
-spec bulk(destination(), index(), type(), doc()) -> response().
bulk(Destination, Index, Type, Doc)
  when is_binary(Index)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc)) ->
    route_call(Destination, {bulk, Index, Type, Doc}, infinity).

%% @equiv refresh(Destination, <<"_all">>)
%% @doc Refresh all indices
-spec refresh(destination()) -> response().
refresh(Destination) ->
    refresh(Destination, ?ALL).

%% @doc Refresh one or more indices
-spec refresh(destination(), index() | [index()]) -> response().
refresh(Destination, Index) when is_binary(Index) ->
    refresh(Destination, [Index]);
refresh(Destination, Indexes) when is_list(Indexes) ->
    route_call(Destination, {refresh, Indexes}, infinity).

%% @doc Flush all indices
%% @equiv flush(Destination, <<"_all">>)
-spec flush(destination()) -> response().
flush(Destination) ->
    flush(Destination, ?ALL).

%% @doc Flush one or more indices
-spec flush(destination(), index() | [index()]) -> response().
flush(Destination, Index) when is_binary(Index) ->
    flush(Destination, [Index]);
flush(Destination, Indexes) when is_list(Indexes) ->
    route_call(Destination, {flush, Indexes}, infinity).

%% @equiv optimize(Destination,  <<"_all">>)
%% @doc Optimize all indices
-spec optimize(destination()) -> response().
optimize(Destination) ->
    optimize(Destination, ?ALL).

%% @doc Optimize one or more indices
-spec optimize(destination(), index() | [index()]) -> response().
optimize(Destination, Index) when is_binary(Index) ->
    optimize(Destination, [Index]);
optimize(Destination, Indexes) when is_list(Indexes) ->
    route_call(Destination, {optimize, Indexes}, infinity).

%% @equiv segments(Destination,  <<"_all">>)
%% @doc Optimize all indices
-spec segments(destination()) -> response().
segments(Destination) ->
    segments(Destination, ?ALL).

%% @doc Optimize one or more indices
-spec segments(destination(), index() | [index()]) -> response().
segments(Destination, Index) when is_binary(Index) ->
    segments(Destination, [Index]);
segments(Destination, Indexes) when is_list(Indexes) ->
    route_call(Destination, {segments, Indexes}, infinity).

%% @equiv clear_cache(Destination, <<"_all">>, [])
%% @doc Clear all the caches
-spec clear_cache(destination()) -> response().
clear_cache(Destination) ->
    clear_cache(Destination, ?ALL, []).

%% @equiv clear_cache(Destination, Indexes, [])
-spec clear_cache(destination(), index() | [index()]) -> response().
clear_cache(Destination, Index) when is_binary(Index) ->
    clear_cache(Destination, [Index], []);
clear_cache(Destination, Indexes) when is_list(Indexes) ->
    clear_cache(Destination, Indexes, []).

%% @equiv clear_cache(Destination, Indexes, [])
-spec clear_cache(destination(), index() | [index()], params()) -> response().
clear_cache(Destination, Index, Params)
  when is_binary(Index), is_list(Params) ->
    clear_cache(Destination, [Index], Params);
clear_cache(Destination, Indexes, Params)
  when is_list(Indexes), is_list(Params) ->
    route_call(Destination, {clear_cache, Indexes, Params}, infinity).


%% @doc Insert a mapping into an ElasticSearch index
-spec put_mapping(destination(), index() | [index()], type(), doc()) ->
    response().
put_mapping(Destination, Index, Type, Doc)
  when is_binary(Index)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc)) ->
    put_mapping(Destination, [Index], Type, Doc);
put_mapping(Destination, Indexes, Type, Doc)
  when is_list(Indexes)
       andalso is_binary(Type)
       andalso (is_binary(Doc) orelse is_map(Doc)) ->
    route_call(Destination, {put_mapping, Indexes, Type, Doc}, infinity).

%% @doc Get a mapping from an ElasticSearch index
-spec get_mapping(destination(), index() | [index()], type()) -> response().
get_mapping(Destination, Index, Type)
  when is_binary(Index) andalso is_binary(Type) ->
    get_mapping(Destination, [Index], Type);
get_mapping(Destination, Indexes, Type)
  when is_list(Indexes) andalso is_binary(Type) ->
    route_call(Destination, {get_mapping, Indexes, Type}, infinity).

%% @doc Delete a mapping from an ElasticSearch index
-spec delete_mapping(destination(), index() | [index()], type()) -> response().
delete_mapping(Destination, Index, Type)
  when is_binary(Index) andalso is_binary(Type) ->
    delete_mapping(Destination, [Index], Type);
delete_mapping(Destination, Indexes, Type)
  when is_list(Indexes) andalso is_binary(Type) ->
    route_call(Destination, {delete_mapping, Indexes, Type}, infinity).

%% @doc Operate on aliases (as compared to 'alias')
-spec aliases(destination(), doc()) -> response().
aliases(Destination, Doc) when (is_binary(Doc) orelse is_map(Doc)) ->
    route_call(Destination, {aliases, Doc}, infinity).

%% @doc Insert an alias (as compared to 'aliases')
-spec insert_alias(destination(), index(), index()) -> response().
insert_alias(Destination, Index, Alias)
  when is_binary(Index)
       andalso is_binary(Alias) ->
    route_call(Destination, {insert_alias, Index, Alias}, infinity).
%% @doc Insert an alias with options(as compared to 'aliases')
-spec insert_alias(destination(), index(), index(), doc()) -> response().
insert_alias(Destination, Index, Alias, Doc)
  when is_binary(Index)
       andalso is_binary(Alias)
       andalso (is_binary(Doc) orelse is_map(Doc)) ->
    route_call(Destination, {insert_alias, Index, Alias, Doc}, infinity).

%% @doc Delete an alias (as compared to 'aliases')
-spec delete_alias(destination(), index(), index()) -> response().
delete_alias(Destination, Index, Alias)
  when is_binary(Index) andalso is_binary(Alias) ->
    route_call(Destination, {delete_alias, Index, Alias}, infinity).

%% @doc Checks if an alias exists (Alias can be a string with a wildcard)
-spec is_alias(destination(), index(), index()) -> boolean().
is_alias(Destination, Index, Alias)
  when is_binary(Index) andalso is_binary(Alias) ->
    Result = route_call(Destination, {is_alias, Index, Alias}, infinity),
    boolean_result(Result).

%% @doc Gets an alias(or more, based on the string)
-spec get_alias(destination(), index(), index()) -> response().
get_alias(Destination, Index, Alias)
  when is_binary(Index) andalso is_binary(Alias) ->
    route_call(Destination, {get_alias, Index, Alias}, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Send the request to the gen_server
-spec route_call(destination(), tuple(), timeout()) -> response().
route_call(Name, Command, Timeout)
  when is_atom(Name); is_pid(Name) ->
    wpool:call(Name, Command, wpool:default_strategy(), Timeout).


-spec boolean_result({ok, pos_integer()}) -> boolean().
boolean_result({ok, Status}) when Status < 400 ->
    true;
boolean_result({ok, _Status}) ->
    false;
boolean_result(Result) ->
    Result.
