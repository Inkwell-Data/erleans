%%%----------------------------------------------------------------------------
%%% Copyright Tristan Sloughter 2019. All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%----------------------------------------------------------------------------

%%% ---------------------------------------------------------------------------
%%% @doc Erleans Grain process manager.
%%% @end
%%% ---------------------------------------------------------------------------
-module(erleans_pm).
-behavior(partisan_gen_server).

-include_lib("partisan/include/partisan.hrl").


%% called by erleans
-export([start_link/0]).
-export([register_name/1]).
-export([unregister_name/1]).
-export([whereis_name/1]).

%% called by this module via spawning
-export([terminator/2]).

%% PARTISAN_GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-include("erleans.hrl").
-include_lib("kernel/include/logger.hrl").

-dialyzer({nowarn_function, register_name/1}).

-define(TOMBSTONE, '$deleted').




%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
start_link() ->
    partisan_gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% -----------------------------------------------------------------------------
%% @doc Registers the calling process with the `id' attribute of `GrainRef'.
%% @end
%% -----------------------------------------------------------------------------
-spec register_name(GrainRef :: erleans:grain_ref()) ->
    ok | {error, {already_in_use, partisan_remote_ref:p()}}.

register_name(GrainRef) ->
    %% We call monitor first as it acts as a barrier
    case monitor(GrainRef) of
        ok ->
            add(GrainRef);
        Error ->
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc It can only be called by the caller
%% @end
%% -----------------------------------------------------------------------------
-spec unregister_name(GrainRef :: erleans:grain_ref()) ->
    ok | no_return().

unregister_name(#{id := _} = GrainRef) ->
    case lookup(GrainRef) of
        undefined ->
            erleans_utils:error(
                badarg, [GrainRef], #{1 => "is not a registered name"}
            );
        ProcessRefList when is_list(ProcessRefList) ->
            Matches = lists:takewhile(
                fun(R) -> partisan:is_local_pid(R, self()) end,
                ProcessRefList
            ),
            case Matches of
                [] ->
                    erleans_utils:error(
                        badarg,
                        [GrainRef],
                        #{1 => "is not registered to the calling process"}
                    );
                [ProcessRef] ->
                    ok = remove(GrainRef, ProcessRef),
                    Pid = partisan_remote_ref:to_term(ProcessRef),
                    true = demonitor(GrainRef, Pid),
                    ok
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec terminator(
    GrainRef :: erleans:grain_ref(), Pid :: partisan_remote_ref:p()) -> ok.

terminator(GrainRef, ProcessRef) ->
    Pid = partisan_remote_ref:to_term(ProcessRef),
    _ = supervisor:terminate_child({erleans_grain_sup, partisan:node()}, Pid),
    remove(GrainRef, ProcessRef).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec whereis_name(GrainRef :: erleans:grain_ref()) ->
    partisan_remote_ref:p() | undefined.

whereis_name(#{placement := stateless} = GrainRef) ->
    whereis_stateless(GrainRef);

whereis_name(#{placement := {stateless, _}} = GrainRef) ->
    whereis_stateless(GrainRef);

whereis_name(#{id := _} = GrainRef) ->
    case lookup(GrainRef) of
        Pids when is_list(Pids) ->
            pick_first_alive(Pids);

        undefined ->
            undefined
    end.



%% =============================================================================
%% PARTISAN_GEN_SERVER BEHAVIOR CALLBACKS
%% =============================================================================



-spec init(Args :: term()) ->
    {ok, State :: term()}.

init(_) ->
    case ets:info(?MODULE, name) of
        undefined ->
            Opts = [
                bag,
                public,
                named_table,
                {write_concurrency, true},
                {read_concurrency, true},
                {decentralized_counters, true}
            ],
            ets:new(?MODULE, Opts);
        _ ->
            ok
    end,
    MS = [{
        %% {{{_, _} = FullPrefix, Key}, NewObj, ExistingObj}
        {{{erleans_pm, grain_ref}, '_'}, '_', '_'},
        [],
        [true]
    }],
    ok = plum_db_events:subscribe(object_update, MS),
    {ok, _State = #{}}.

-spec handle_call(Request :: term(), From :: {pid(),
    Tag :: term()}, State :: term()) ->
    {reply, Reply :: term(), NewState :: term()}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

-spec handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()}.

handle_cast({monitor, Pid}, State) when is_pid(Pid) ->
    _ = erlang:monitor(process, Pid),
    {noreply, State};

handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(Message :: term(), State :: term()) ->
    {noreply, NewState :: term()}.

handle_info({'DOWN', _MRef, process, Pid, _}, State) ->
    lists:foreach(
        fun({_, GrainRef}) ->
            remove(GrainRef, Pid),
            true = ets:delete(?MODULE, Pid),
            true = ets:delete(?MODULE, GrainRef)
        end,
        ets:lookup(?MODULE, Pid)
    ),
    {noreply, State};

handle_info(
    {plum_db_event, object_update, {{{_, _}, GrainRef}, Obj, PrevObj}},
    State) ->
    ?LOG_DEBUG(#{
        message => "plum_db_event object_update received",
        object => Obj,
        prev_obj => PrevObj
    }),

    %% REVIEW we are using lww here, shuoldn't we use resolver/2?
    Resolved = plum_db_object:resolve(Obj, fun resolver/2),

    case plum_db_object:value(Resolved) of
        ProcessRefs when is_list(ProcessRefs), length(ProcessRefs) > 1 ->
            %% we need to remove dead processes first to avoid the case when we
            %% kill one that returns false in the belief that there's at least
            %% one remaining but that could actually be dead already
            Alive = remove_dead(ProcessRefs),
            _Pid = terminate_duplicates(GrainRef, Alive);
        _ ->
            ok
    end,
    {noreply, State};

handle_info(_, State) ->
    {noreply, State}.

-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: term()) ->
    term().

terminate(_Reason, _State) ->
    ok.


%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
terminate_duplicates(GrainRef, ProcessRefs) ->
    Fun = fun
        (ProcessRef, {1, undefined}) ->
            %% keep the last one as non of them replied true before
            {0, ProcessRef};

        (ProcessRef, {Rem, undefined}) ->
            %% checks if grain activation is optimal
            %% This won't cause a loop or an activation
            %% because is_location_right/2 is not an erleans_grain:call/2,3
            case erleans_grain:is_location_right(GrainRef, ProcessRef) of
                true ->
                    %% this process will be selected
                    {Rem - 1, ProcessRef};
                false ->
                    %% terminate the grain as we still have at least
                    %% one more in the list to check
                    ok = terminate_grain(GrainRef, ProcessRef),
                    {Rem - 1, undefined};
                noproc ->
                    %% grain activation deactivated, or
                    %% some other process terminated it
                    remove(GrainRef, ProcessRef),
                    {Rem - 1, undefined}
            end;

        (ProcessRef, {Rem, Keep}) ->
            %% we already have the one to keep, so we terminate the rest
            ok = terminate_grain(GrainRef, ProcessRef),
            {Rem - 1, Keep}
    end,

    {_, ProcessRef} = lists:foldl(
        Fun, {length(ProcessRefs), undefined}, ProcessRefs
    ),
    ProcessRef.


%% @private
terminate_grain(GrainRef, ProcessRef) ->
    %% spawn a terminator on the same node where the grain activation is
    _ = partisan:spawn(
        partisan:node(ProcessRef), ?MODULE, terminator, [GrainRef, ProcessRef]
    ),
    ok.



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
pick_first_alive([]) ->
    undefined;

pick_first_alive([ProcessRef | ProcessRefs]) ->
    case is_proc_alive(ProcessRef) of
        true ->
            ProcessRef;
        false ->
            pick_first_alive(ProcessRefs)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
whereis_stateless(GrainRef) ->
    case gproc_pool:pick_worker(GrainRef) of
        false ->
            undefined;
        Pid ->
            partisan_remote_ref:from_term(Pid)
    end.




%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add(GrainRef :: erleans:grain_ref()) ->
    ok | {error, {already_in_use, partisan_remote_ref:p()}}.

add(#{id := _} = GrainRef) ->
    PKey = {?MODULE, grain_ref},
    ProcessRef = partisan:self(),
    Opts = [
        {resolver, fun resolver/2},
        {allow_put, false} %% we avoid resolving on get
    ],

    case plum_db:get(PKey, GrainRef, Opts) of
        undefined ->
            plum_db:put(PKey, GrainRef, [ProcessRef]);

        [] ->
            plum_db:put(PKey, GrainRef, [ProcessRef]);

        L when is_list(L) ->
            %% We have multiple instanciations of this Grain or we have a
            %% temporal inconsistency, we add the new grain process reference
            %% as long as the existing instances are not local.
            LocalInstances = lists:filter(
                fun(PRef) -> partisan:is_local_pid(PRef) end,
                L
            ),

            case LocalInstances of
                [] ->
                    plum_db:put(PKey, GrainRef, lists:usort(L ++ [ProcessRef]));
                [H|_] when H == self() ->
                    %% Makes this call idempotent
                    ok;
                [H|_] ->
                    {error, {already_in_use, H}}
            end
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec remove(
    GrainRef :: erleans:grain_ref(),
    Pid :: pid() | partisan_remote_ref:p()) -> ok.

remove(GrainRef, Pid) when is_pid(Pid) ->
    remove(GrainRef, partisan_remote_ref:from_term(Pid));

remove(#{id := _} = GrainRef, ProcessRef) ->
    PKey = {?MODULE, grain_ref},
    Opts = [
        {resolver, fun resolver/2},
        {allow_put, false} % We will manually put as we need to remove Pid
    ],
    case plum_db:get(PKey, GrainRef, Opts) of
        undefined ->
            ok;
        [] ->
            ok;
        L when is_list(L) ->
            do_remove(GrainRef, L, ProcessRef)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
do_remove(GrainRef, L0, ProcessRef) ->
    PKey = {?MODULE, grain_ref},

    case L0 -- [ProcessRef] of
        [] ->
            ok = plum_db:delete(PKey, GrainRef);
        L1 ->
            ok = plum_db:put(PKey, GrainRef, lists:usort(L1))
    end.


%% -----------------------------------------------------------------------------
%% @private
%% %% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec lookup(GrainRef :: erleans:grain_ref()) ->
    list(partisan_remote_ref:p()) | undefined.

lookup(#{id := _} = GrainRef) ->
    Opts = [
        {resolver, fun resolver/2},
        {allow_put, true} % This will make a put to store the resolved value
    ],
    case plum_db:get({?MODULE, grain_ref}, GrainRef, Opts) of
        [] ->
            undefined;
        Other ->
            Other
    end.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
resolver(?TOMBSTONE, ?TOMBSTONE) ->
    ?TOMBSTONE;

resolver(?TOMBSTONE, L) when is_list(L) ->
    maybe_tombstone(remove_dead(L));

resolver(L, ?TOMBSTONE) when is_list(L) ->
    maybe_tombstone(remove_dead(L));

resolver(L1, L2) when is_list(L1) andalso is_list(L2) ->
    %% Lists are sorted already as we sort them every time we do a put
    maybe_tombstone(
        remove_dead(lists:umerge(L1, L2))
    ).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
remove_dead(ProcessRefs) ->
    lists:filter(
        fun(ProcessRef) -> is_proc_alive(ProcessRef) end, ProcessRefs
    ).

is_proc_alive(ProcessRef) ->
    case partisan:is_local_pid(ProcessRef) of
        true ->
            partisan:is_process_alive(ProcessRef);
        false ->
            Node = partisan:node(ProcessRef),
            Result = partisan_rpc:call(
                Node, partisan, is_process_alive, [ProcessRef], 5000
            ),
            case Result of
                {badrpc, _Reason} ->
                    %% Reason can be nodedown, timeout, etc.
                    false;
                Result ->
                    Result
            end
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
maybe_tombstone([]) ->
    ?TOMBSTONE;

maybe_tombstone(L) ->
    L.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec monitor(GrainRef :: erleans:grain_ref()) ->
    ok | {error, {already_in_use, partisan_remote_ref:p()}}.

monitor(GrainRef) ->
    Pid = self(),
    %% We guarantee that locally it can only be one activation
    case ets:insert_new(?MODULE, [{Pid, GrainRef}, {GrainRef, Pid}]) of
        true ->
            gen_server:cast(?MODULE, {monitor, Pid});
        false ->
            [Existing] = ets:lookup_element(?MODULE, GrainRef, 2),
            {error, {already_in_use, partisan_remote_ref:from_term(Existing)}}
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(GrainRef :: erleans:grain_ref(), Pid :: pid()) -> true.

demonitor(#{id := _} = GrainRef, Pid) when is_pid(Pid) ->
    true = ets:delete_object(?MODULE, {Pid, GrainRef}),
    ets:delete_object(?MODULE, {GrainRef, Pid}).

