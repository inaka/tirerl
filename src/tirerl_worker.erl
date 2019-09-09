%% @hidden
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
        #{pool_name => tirerl:pool_name(),
          connection_options => tirerl:params(),
          base_url => binary()
         }.

-type request() ::
        #{method => atom(),
          uri => string() | binary(),
          parameters => map(),
          headers =>  list(),
          body => string() | binary()
         }.

-type response() :: error() | {ok, map() | pos_integer()}.

-type internal_response() ::
        #{status => integer(),
          headers => list(),
          body => undefined | string() | binary()
         }.

-type error()      :: {error, Reason :: any()}.
-type json()       :: map().

-export_type([error/0, response/0]).

-define(DEFAULT_HOST, <<"localhost">>).
-define(DEFAULT_PORT, 9200).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Pool API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc To start up a 'simple' client
-spec start(tirerl:params()) -> {ok, pid()}.
start(Options) when is_list(Options) ->
    gen_server:start(?MODULE, Options, []).

%% @doc Stop this gen_server
-spec stop(tirerl:destination()) -> ok | error().
stop(ServerRef) ->
    gen_server:call(ServerRef, {stop}, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-spec init(tirerl:params()) -> {ok, state()}.
init(ConnOpts) ->
    Host = proplists:get_value(host, ConnOpts, ?DEFAULT_HOST),
    Port = proplists:get_value(port, ConnOpts, ?DEFAULT_PORT),

    {ok, #{pool_name => undefined,
           connection_options => ConnOpts,
           base_url => <<Host/binary, ":", (integer_to_binary(Port))/binary>>}}.

-spec handle_call({stop} | term(), _, state()) ->
        {stop, normal, ok, state()} |
        {reply, error() | {ok, map() | pos_integer()}, state()} |
        {stop, unhandled_call, state()}.
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

-spec handle_cast(_, state()) -> {stop, unhandled_info, state()}.
handle_cast(_Request, State) ->
    {stop, unhandled_info, State}.

-spec handle_info(_, state()) -> {stop, unhandled_info, state()}.
handle_info(_Info, State) ->
    {stop, unhandled_info, State}.

-spec terminate(_, state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, state(), _) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec do_request(request(), state()) ->
    {ok, json()} | {error, term()}.
do_request(Req, #{base_url := BaseUrl}) ->
    #{method := Method, uri := Uri} = Req,
    Body = maps:get(body, Req, <<>>),
    Headers = maps:get(headers, Req,
                       [{<<"Content-Type">>, <<"application/json">>}]),

    Body1 = case Body of
                Body when is_binary(Body) -> Body;
                _ -> jsx:encode(Body)
            end,

    FullUri =
        case Uri of
            [$/ | _] -> Uri;
            Uri -> [$/ | Uri]
        end,

    URL = <<BaseUrl/binary, (list_to_binary(FullUri))/binary>>,

    try
        Response = case hackney:request(Method, URL, Headers, Body1) of
            {ok, StatusCode, _Headers, ClientRef} ->
                Response1 = #{status_code => StatusCode},
                case hackney:body(ClientRef) of
                    {ok, ResponseBody} ->
                        {ok, Response1#{body => ResponseBody}};
                    {error, _} ->
                        {ok, Response1}
                end;
            {ok, StatusCode, _Headers} ->
                {ok, #{status_code => StatusCode}};
            {error, Reason} ->
                {error, Reason}
        end,
        process_response(Response)
    catch
        error:Reason1 ->
            {error, Reason1}
    end.

-spec process_response({ok, internal_response()} | error()) -> response().
process_response({error, _} = Response) ->
    Response;
process_response({ok, #{status_code := Status, body := Body} = Response}) ->
    try
        Json = jsx:decode(Body, [return_maps]),
        case Status of
            Status when Status < 400 ->
                {ok, Json};
            _ ->
                {error, Json}
        end
    catch
        error:badarg -> {error, Response}
    end;
process_response({ok, #{status_code := Status} = Response}) ->
    case Status of
        Status when Status < 500 ->
            {ok, Status};
        _ ->
            {error, Response}
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
    Uri = make_uri([IndexList, TypeList, ?DELETE_BY_QUERY], Params),
    #{method => post, uri => Uri, body => Doc};

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
    #{method => put,
      uri => make_uri([Index, Type, Id], Params),
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
    #{method => get, uri => make_uri([Index, Type, Id], Params)};

make_request({mget_doc, Index, Type, Doc}) ->
    Uri = make_uri([Index, Type, ?MGET], []),
    #{method => get, uri => Uri, body => Doc};

make_request({delete_doc, Index, Type, Id, Params}) ->
    #{method => delete, uri => make_uri([Index, Type, Id], Params)};

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
    #{method => put,
      uri => join([IndexList, ?MAPPING, Type], <<"/">>),
      body => Doc};

make_request({get_mapping, Indexes, Type}) ->
    IndexList = join(Indexes, <<", ">>),
    #{method => get, uri => join([IndexList, ?MAPPING, Type], <<"/">>)};

make_request({delete_mapping, Indexes, Type}) ->
    IndexList = join(Indexes, <<", ">>),
    #{method => delete, uri => join([IndexList, ?MAPPING, Type], <<"/">>)};

make_request({aliases, Doc}) ->
    Uri = ?ALIASES,
    #{method => post,
      uri => Uri,
      body => Doc};

make_request({insert_alias, Index, Alias}) ->
    #{method => put, uri => join([Index, ?ALIAS, Alias], <<"/">>)};

make_request({insert_alias, Index, Alias, Doc}) ->
    #{method => put, uri => join([Index, ?ALIAS, Alias], <<"/">>), body => Doc};

make_request({delete_alias, Index, Alias}) ->
    #{method => delete, uri => join([Index, ?ALIAS, Alias], <<"/">>)};

make_request({is_alias, Index, Alias}) ->
    #{method => head, uri => join([Index, ?ALIAS, Alias], <<"/">>)};

make_request({get_alias, Index, Alias}) ->
    #{method => get, uri => join([Index, ?ALIAS, Alias], <<"/">>)}.

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
