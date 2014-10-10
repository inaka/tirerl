-module(tirerl_worker).

-behaviour(gen_server).

-include("tirerl.hrl").

%% API
-export([start/1, stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-type state() ::
        #{connection => connection(),
          connection_options => tirerl:params(),
          pool_name => tirerl:pool_name()
         }.

-type request() ::
        #{method => atom(),
          uri => string() | binary(),
          parameters => map(),
          headers =>  map(),
          body => string() | binary()
         }.

-type response() ::
        #{status => integer(),
          headers => map(),
          body => undefined | string() | binary()
         }.

-type error()           :: {error, Reason :: any()}.
-type exception()       :: {exception, Reason :: any()}.
-type connection()      :: pid().

-define(DEFAULT_HOST, "localhost").
-define(DEFAULT_PORT, 9200).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Pool API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc To start up a 'simple' client
-spec start(tirerl:params()) -> {ok, pid()}.
start(Options) when is_list(Options) ->
    gen_server:start(?MODULE, {undefined, Options}, []).

%% @doc Stop this gen_server
-spec stop(tirerl:destination()) -> ok | error().
stop(ServerRef) ->
    gen_server:call(ServerRef, {stop}, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init({Name, ConnOpts}) ->
    {ok, #{pool_name => Name,
           connection_options => ConnOpts,
           connection => connection(ConnOpts)}}.

handle_call({stop}, _From, State) ->
    {stop, normal, ok, State};

handle_call(Msg, _From, State) ->
    try
        Request = make_request(Msg),
        Response = do_request(Request, State),
        {reply, Response, State}
    catch
        error:function_clause ->
            {stop, unhandled_call, State}
    end.

handle_cast(_Request, State) ->
    {stop, unhandled_info, State}.

handle_info(_Info, State) ->
    {stop, unhandled_info, State}.

terminate(_Reason, #{connection := Connection}) ->
    shotgun:close(Connection),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% @doc Build a new connection
-spec connection(tirerl:params()) -> connection().
connection(ConnectionOptions) ->
    Host = proplists:get_value(host, ConnectionOptions, ?DEFAULT_HOST),
    Port = proplists:get_value(port, ConnectionOptions, ?DEFAULT_PORT),
    {ok, Pid} = shotgun:open(Host, Port),
    Pid.

-spec do_request(request(), state()) ->
    {connection(),  {ok, response()} | error()}
    | {error, closed, state()}
    | {error, econnrefused, state()}.
do_request(Req, State = #{connection := Connection}) ->
    #{method := Method, uri := Uri} = Req,
    Body = maps:get(body, Req, <<>>),
    Headers = maps:get(headers, Req, #{}),

    Body1 = case Body of
                Body when is_binary(Body) -> Body;
                _ -> jiffy:encode(Body)
            end,

    try
        Response = case Method of
                       M when M == put;
                              M == post;
                              M == patch ->
                           shotgun:Method(Connection, Uri, Headers, Body1, #{});
                       _ ->
                           shotgun:Method(Connection, Uri, Headers, #{})
                   end,
        process_response(Response)
    catch
        error:badarg ->
            {error, badarg};
        error:{case_clause, {error, closed}} ->
            {error, closed, State};
        error:{case_clause, {error, econnrefused}} ->
            {error, econnrefused, State}
    end.

-spec process_response({ok, response()} | error() | exception()) ->
    response().
process_response({error, _} = Response) ->
    Response;
process_response({ok, #{status_code := Status, body := Body}}) ->
    try
        #{status => Status,
          body => jiffy:decode(Body, [return_maps])}
    catch
        error:badarg -> [{status, Status}, {body, Body}]
    end.

make_request({health}) ->
    #{method => get, uri => ?HEALTH};

make_request({state, Params}) ->
    Uri = make_uri([?STATE], Params),
    #{method => get, uri => Uri};

make_request({nodes_info, NodeNames, Params}) ->
    NodeNameList = join(NodeNames, <<", ">>),
    Uri = make_uri([?NODES, NodeNameList], Params),
    #{method => get, uri => Uri};

make_request({nodes_stats, NodeNames, Params}) ->
    NodeNameList = join(NodeNames, <<", ">>),
    Uri = make_uri([?NODES, NodeNameList, ?STATS], Params),
    #{method => get, uri => Uri};

make_request({status, Index}) ->
    IndexList = join(Index, <<", ">>),
    Uri = join([IndexList, ?STATUS], <<"/">>),
    #{method => get, uri => Uri};

make_request({indices_stats, Index}) ->
    IndexList = join(Index, <<", ">>),
    Uri = join([IndexList, ?INDICES_STATS], <<"/">>),
    #{method => get, uri => Uri};

make_request({create_index, Index, Doc}) ->
    #{method => put, uri => Index, body => Doc};

make_request({delete_index, Index}) ->
    IndexList = join(Index, <<", ">>),
    #{method => delete, uri => IndexList};

make_request({open_index, Index}) ->
    Uri = join([Index, ?OPEN], <<"/">>),
    #{method => post, uri => Uri};

make_request({close_index, Index}) ->
    Uri = join([Index, ?CLOSE], <<"/">>),
    #{method => post, uri => Uri};

make_request({count, Index, Type, Doc, Params}) ->
    IndexList = join(Index, <<", ">>),
    TypeList = join(Type, <<", ">>),
    Uri = make_uri([IndexList, TypeList, ?COUNT], Params),
    #{method => get, uri => Uri, body => Doc};

make_request({delete_by_query, Index, Type, Doc, Params}) ->
    IndexList = join(Index, <<", ">>),
    TypeList = join(Type, <<", ">>),
    Uri = make_uri([IndexList, TypeList, ?QUERY], Params),
    #{method => delete, uri => Uri, body => Doc};

make_request({is_index, Index}) ->
    IndexList = join(Index, <<", ">>),
    #{method => head, uri => IndexList};

make_request({is_type, Index, Type}) ->
    IndexList = join(Index, <<", ">>),
    TypeList = join(Type, <<", ">>),
    Uri = join([IndexList, TypeList], <<"/">>),
    #{method => head, uri => Uri};

make_request({insert_doc, Index, Type, undefined, Doc, Params}) ->
    Uri = make_uri([Index, Type], Params),
    #{method => post,
      uri => Uri,
      body => Doc};

make_request({insert_doc, Index, Type, Id, Doc, Params}) ->
    Uri = make_uri([Index, Type, Id], Params),
    #{method => put,
      uri => Uri,
      body => Doc};

make_request({update_doc, Index, Type, Id, Doc, Params}) ->
    Uri = make_uri([Index, Type, Id, ?UPDATE], Params),
    #{method => post,
      uri => Uri,
      body => Doc};

make_request({is_doc, Index, Type, Id}) ->
    Uri = make_uri([Index, Type, Id], []),
    #{method => head, uri => Uri};

make_request({get_doc, Index, Type, Id, Params}) ->
    Uri = make_uri([Index, Type, Id], Params),
    #{method => get, uri => Uri};

make_request({mget_doc, Index, Type, Doc}) ->
    Uri = make_uri([Index, Type, ?MGET], []),
    #{method => get, uri => Uri, body => Doc};

make_request({delete_doc, Index, Type, Id, Params}) ->
    Uri = make_uri([Index, Type, Id], Params),
    #{method => delete, uri => Uri};

make_request({search, Index, Type, Doc, Params}) ->
    Uri = make_uri([Index, Type, ?SEARCH], Params),
    #{method => get,
      uri => Uri,
      body => Doc};

make_request({bulk, <<>>, <<>>, Doc}) ->
    Uri = make_uri([?BULK], []),
    #{method => post,
      uri => Uri,
      body => Doc};
make_request({bulk, Index, <<>>, Doc}) ->
    Uri = make_uri([Index, ?BULK], []),
    #{method => post,
      uri => Uri,
      body => Doc};
make_request({bulk, Index, Type, Doc}) ->
    Uri = make_uri([Index, Type, ?BULK], []),
    #{method => post,
      uri => Uri,
      body => Doc};

make_request({refresh, Index}) ->
    IndexList = join(Index, <<", ">>),
    Uri = join([IndexList, ?REFRESH], <<"/">>),
    #{method => post, uri => Uri};

make_request({flush, Index}) ->
    IndexList = join(Index, <<", ">>),
    Uri = join([IndexList, ?FLUSH], <<"/">>),
    #{method => post, uri => Uri};

make_request({optimize, Index}) ->
    IndexList = join(Index, <<", ">>),
    Uri = join([IndexList, ?OPTIMIZE], <<"/">>),
    #{method => post, uri => Uri};

make_request({segments, Index}) ->
    IndexList = join(Index, <<", ">>),
    Uri = join([IndexList, ?SEGMENTS], <<"/">>),
    #{method => get, uri => Uri};

make_request({clear_cache, Index, Params}) ->
    IndexList = join(Index, <<", ">>),
    Uri = make_uri([IndexList, ?CLEAR_CACHE], Params),
    #{method => post, uri => Uri};

make_request({put_mapping, Indexes, Type, Doc}) ->
    IndexList = join(Indexes, <<", ">>),
    Uri = join([IndexList, ?MAPPING, Type], <<"/">>),
    #{method => put,
      uri => Uri,
      body => Doc};

make_request({get_mapping, Indexes, Type}) ->
    IndexList = join(Indexes, <<", ">>),
    Uri = join([IndexList, ?MAPPING, Type], <<"/">>),
    #{method => get, uri => Uri};

make_request({delete_mapping, Indexes, Type}) ->
    IndexList = join(Indexes, <<", ">>),
    Uri = join([IndexList, ?MAPPING, Type], <<"/">>),
    #{method => delete, uri => Uri};

make_request({aliases, Doc}) ->
    Uri = ?ALIASES,
    #{method => post,
      uri => Uri,
      body => Doc};

make_request({insert_alias, Index, Alias}) ->
    Uri = join([Index, ?ALIAS, Alias], <<"/">>),
    #{method => put, uri => Uri};

make_request({insert_alias, Index, Alias, Doc}) ->
    Uri = join([Index, ?ALIAS, Alias], <<"/">>),
    #{method => put, uri => Uri, body => Doc};

make_request({delete_alias, Index, Alias}) ->
    Uri = join([Index, ?ALIAS, Alias], <<"/">>),
    #{method => delete, uri => Uri};

make_request({is_alias, Index, Alias}) ->
    Uri = join([Index, ?ALIAS, Alias], <<"/">>),
    #{method => head, uri => Uri};

make_request({get_alias, Index, Alias}) ->
    Uri = join([Index, ?ALIAS, Alias], <<"/">>),
    #{method => get, uri => Uri}.

-spec join_list_sep([binary()], binary()) -> [any()].
join_list_sep([Head | Tail], Sep) ->
    join_list_sep(Tail, Sep, [Head]);
join_list_sep([], _Sep) ->
    [].
join_list_sep([Head | Tail], Sep, Acc) ->
    join_list_sep(Tail, Sep, [Head, Sep | Acc]);
join_list_sep([], _Sep, Acc) ->
    lists:reverse(Acc).

%% @doc Make a complete URI based on the tokens and props
make_uri(BaseList, PropList) ->
    Base = join(BaseList, <<"/">>),
    case PropList of
        [] ->
            Base;
        PropList ->
            Props = uri_params_encode(PropList),
            join([Base, Props], <<"?">>)
    end.

%% @doc Join a a list of strings into one string, adding a separator between
%%      each string.
-spec join([binary()], Sep::binary()) -> binary().
join(List, Sep) when is_list(List) ->
    list_to_binary(join_list_sep(List, Sep)).

% Thanks to https://github.com/tim/erlang-oauth.git
-spec uri_params_encode([tuple()]) -> string().
uri_params_encode(Params) ->
    intercalate("&", [uri_join([K, V], "=") || {K, V} <- Params]).

uri_join(Values, Separator) ->
    string:join([uri_encode(Value) || Value <- Values], Separator).

intercalate(Sep, Xs) ->
    lists:concat(intersperse(Sep, Xs)).

intersperse(_, []) ->
    [];
intersperse(_, [X]) ->
    [X];
intersperse(Sep, [X | Xs]) ->
    [X, Sep | intersperse(Sep, Xs)].

uri_encode(Term) when is_binary(Term) ->
    uri_encode(binary_to_list(Term));
uri_encode(Term) when is_integer(Term) ->
    uri_encode(integer_to_list(Term));
uri_encode(Term) when is_atom(Term) ->
    uri_encode(atom_to_list(Term));
uri_encode(Term) when is_list(Term) ->
    uri_encode(lists:reverse(Term, []), []).

-define(IS_ALPHANUM(C), C >= $A, C =< $Z; C >= $a, C =< $z; C >= $0, C =< $9).

uri_encode([X | T], Acc)
  when ?IS_ALPHANUM(X); X =:= $-; X =:= $_; X =:= $.; X =:= $~ ->
    uri_encode(T, [X | Acc]);
uri_encode([X | T], Acc) ->
    NewAcc = [$%, dec2hex(X bsr 4), dec2hex(X band 16#0f) | Acc],
    uri_encode(T, NewAcc);
uri_encode([], Acc) ->
    Acc.

-compile({inline, [{dec2hex, 1}]}).

dec2hex(N) when N >= 10 andalso N =< 15 ->
    N + $A - 10;
dec2hex(N) when N >= 0 andalso N =< 9 ->
    N + $0.
