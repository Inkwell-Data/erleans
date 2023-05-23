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

-define(PDB_PREFIX, {?MODULE, grain_ref}).
-define(TAB, ?MODULE).
-define(TOMBSTONE, '$deleted').

%% This server may receive a huge amount of messages.
%% Make sure that they are stored off heap to avoid exessive GCs.
-define(SPAWN_OPTS, [{spawn_opt, [{message_queue_data, off_heap}]}]).


%% called by erleans
-export([start_link/0]).
-export([register_name/1]).
-export([unregister_name/1]).
-export([whereis_name/1]).
-export([whereis_name/2]).

%% called by this module via spawning
-export([terminator/2]).

%% PARTISAN_GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_continue/2]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-include("erleans.hrl").
-include_lib("kernel/include/logger.hrl").

-dialyzer({nowarn_function, register_name/1}).





%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
start_link() ->
    partisan_gen_server:start_link({local, ?MODULE}, ?MODULE, [], ?SPAWN_OPTS).


%% -----------------------------------------------------------------------------
%% @doc Registers the calling process with the `id' attribute of `GrainRef'.
%% This call is serialised through a server process.
%% @end
%% -----------------------------------------------------------------------------
-spec register_name(GrainRef :: erleans:grain_ref()) ->
    ok | {error, {already_in_use, partisan_remote_ref:p()}}.

register_name(GrainRef) ->
    partisan_gen_server:call(?MODULE, {register_name, GrainRef}).



%% -----------------------------------------------------------------------------
%% @doc It can only be called by the caller
%% This call is serialised through a server process.
%% @end
%% -----------------------------------------------------------------------------
-spec unregister_name(GrainRef :: erleans:grain_ref()) ->
    ok | no_return().

unregister_name(#{id := _} = GrainRef) ->
    partisan_gen_server:call(?MODULE, {register_name, GrainRef}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec terminator(
    GrainRef :: erleans:grain_ref(), Pid :: partisan_remote_ref:p()) -> ok.

terminator(GrainRef, ProcessRef) ->
    Pid = partisan_remote_ref:to_term(ProcessRef),
    _ = partisan_gen_supervisor:terminate_child(
        {erleans_grain_sup, partisan:node()}, Pid
    ),
    global_remove(GrainRef, ProcessRef).


%% -----------------------------------------------------------------------------
%% @doc Returns a process reference for `GrainRef' unless there is no reference
%% in which case returns `undefined'. This function calls
%% {@link erlans_pm:whereis_name/2} passing the options `[safe]'.
%%
%% Notice that as we use an eventually consistent model and temporarily support
%% duplicated activations for a grain reference in different locations we could
%% have multiple instances in the global registry. This function chooses the
%% first reference in the list that represents a live process. Checking for
%% liveness incurs in a remote call for remote processes and thus can be
%% expensive in the presence of multiple instanciations. If you prefer to avoid
%% this check you can call {@link erlans_pm:whereis_name/2} passing [unsafe] as
%% the second argument.
%% @end
%% -----------------------------------------------------------------------------
-spec whereis_name(GrainRef :: erleans:grain_ref()) ->
    partisan_remote_ref:p() | undefined.

whereis_name(GrainRef) ->
    whereis_name(GrainRef, [safe]).


%% -----------------------------------------------------------------------------
%% @doc Returns a process reference for `GrainRef' unless there is no reference
%% in which case returns `undefined'.
%% If the option `[safe]` is used it will return the process reference only if
%% its process is alive. Checking for liveness on remote processes incurs a
%% rmeote call. When there is no connection to the node in which the
%% process lives, it is deemed dead.
%%
%% If Opts is `[]` or `[unsafe]` it will not check for liveness.
%% @end
%% -----------------------------------------------------------------------------
-spec whereis_name(GrainRef :: erleans:grain_ref(), Opts :: [safe | unsafe]) ->
    partisan_remote_ref:p() | undefined.

whereis_name(#{placement := stateless} = GrainRef, _) ->
    whereis_stateless(GrainRef);

whereis_name(#{placement := {stateless, _}} = GrainRef, _) ->
    whereis_stateless(GrainRef);

whereis_name(GrainRef, []) ->
    whereis_name(GrainRef, [unsafe]);

whereis_name(GrainRef, [_|T] = L) when T =/= [] ->
    case lists:member(safe, L) of
        true ->
            whereis_name(GrainRef, [safe]);
        false ->
            whereis_name(GrainRef, [unsafe])
    end;

whereis_name(#{id := _} = GrainRef, [Flag]) ->
    Opts = [
        {resolver, fun resolver/2},
        {allow_put, false}
    ],
    case global_lookup(GrainRef, Opts) of
        ProcessRefs when is_list(ProcessRefs), Flag == safe ->
            pick_first_alive(ProcessRefs);

        [ProcessRef|_]  when Flag == unsafe ->
            ProcessRef;

        undefined ->
            undefined
    end.



%% =============================================================================
%% PARTISAN_GEN_SERVER BEHAVIOR CALLBACKS
%% =============================================================================



-spec init(Args :: term()) -> {ok, State :: term()}.

init(_) ->
    %% Trap exists otherwise terminate/1 won't be called when shutdown by
    %% supervisor
    erlang:process_flag(trap_exit, true),

    case ets:info(?TAB, name) of
        undefined ->
            Opts = [
                set,
                protected,
                named_table,
                {write_concurrency, true},
                {read_concurrency, true},
                {decentralized_counters, true}
            ],
            ets:new(?TAB, Opts);
        _ ->
            ok
    end,
    MS = [{
        %% {{FullPrefix, Key}, NewObj, ExistingObj}
        {{?PDB_PREFIX, '_'}, '_', '_'},
        [],
        [true]
    }],
    ok = plum_db_events:subscribe(object_update, MS),
    State = #{},
    {ok, State, {continue, global_cleanup}}.


handle_continue(global_cleanup, State) ->
    %% We remove all local references from the global registry. These would be
    %% references that we were not able to remove on terminate/2 e.g. network
    %% split when shutdown/crash occured.
    Fun = fun
        ({_, []}) ->
            ok;

        ({GrainRef, ProcessRefs0}) when is_list(ProcessRefs0) ->
            %% We update the record by removing any local reference.
            %% We do not need to check with the local registry (ets table) as
            %% this call occurs before any other process could call
            %% register_name.
            ProcessRefs = lists:usort(exclude_local(ProcessRefs0)),
            plum_db:put(?PDB_PREFIX, GrainRef, ProcessRefs)
    end,

    %% We use the exclude_local_resolver/2 to avoid making a remote call to
    %% check on process liveness for remote process as we only care about
    %% removing instances of previous local processes
    Opts = [
        {remove_tombstones, true},
        {resolver, fun exclude_local_resolver/2},
        {allow_put, false}
    ],

    ok = plum_db:foreach(Fun, ?PDB_PREFIX, Opts),

    {noreply, State};

handle_continue(_, State) ->
    {noreply, State}.



handle_call({register_name, GrainRef}, {Caller, _}, State)
when is_pid(Caller) ->
    Reply = case exclude_unreachable(global_lookup(GrainRef)) of
        [] ->
            %% We call monitor first as it acts as a memory barrier,
            %% serialising all local concurrent registrations.
            case local_add(GrainRef, Caller) of
                ok ->
                    global_add(GrainRef, Caller);

                {error, {already_in_use, _}} = Error ->
                    %% Some other process beat us in calling local_add
                    Error
            end;

        [ProcessRef|_] ->
            {error, {already_in_use, ProcessRef}}
    end,

    {reply, Reply, State};

handle_call({register_name, _}, _From, State) ->
    %% From is a partisan_process_ref
    Error = {error, not_local},
    {reply, Error, State};

handle_call({unregister_name, GrainRef}, {Caller, _}, State)
when is_pid(Caller) ->
    Reply = case ets:lookup(?TAB, GrainRef) of
        [] ->
            erleans_utils:error(
                badarg, [GrainRef], #{
                    1 => "is not a registered name"
                }
            );
        [{GrainRef, Pid, MRef}] when Pid == Caller ->
            _ = erlang:demonitor(MRef),
            true = local_remove(GrainRef, Pid),
            ok = global_remove(GrainRef, Pid);

        [{GrainRef, Pid, _}] when Pid =/= Caller ->
            erleans_utils:error(
                badarg, [GrainRef], #{
                    1 => "is not a registered name for the calling process"
                }
            )
    end,

    {reply, Reply, State};

handle_call({unregister_name, _}, _From, State) ->
    %% From is a partisan_process_ref, thus not local
    Error = {error, not_local},
    {reply, Error, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


-spec handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()}.

handle_cast(_Request, State) ->
    {noreply, State}.


-spec handle_info(Message :: term(), State :: term()) ->
    {noreply, NewState :: term()}.

handle_info({'DOWN', MRef, process, Pid, _}, State) ->
    ?LOG_INFO("Process down ~p", [{Pid, MRef}]),
    case ets:lookup(?TAB, Pid) of
        [{Pid, GrainRef, MRef}] ->
            ok = local_remove(GrainRef, Pid),
            ok = global_remove(GrainRef, Pid);
        _ ->
            ok
    end,

    {noreply, State};

handle_info(
    {plum_db_event, object_update, {{{_, _}, GrainRef}, Obj, PrevObj}},
    State) ->
    ?LOG_DEBUG(#{
        message => "plum_db_event object_update received",
        object => Obj,
        prev_obj => PrevObj
    }),

    Resolved = plum_db_object:resolve(Obj, fun exclude_dead_resolver/2),

    case plum_db_object:value(Resolved) of
        ProcessRefs when is_list(ProcessRefs) ->
            %% we need to remove dead processes first to avoid the case when we
            %% kill one that returns false in the belief that there's at least
            %% one remaining but that could actually be dead already
            ok = terminate_duplicates(GrainRef, ProcessRefs);
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
    ok = unregister_all().



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
terminate_duplicates(GrainRef, ProcessRefs0) ->
    %% We first remove all process known to be dead (non-reachable processes
    %% are assumed to be alive).
    %% We still need to call this function, because resolve/2 will only be
    %% invoked when there is a conflict
    ProcessRefs1 = exclude_dead(ProcessRefs0),

    Fun = fun
        (ProcessRef, {1, false, Kill, Keep}) ->
            %% We keep the last one as none of the previous ones are optimal
            {0, true, Kill, [ProcessRef|Keep]};

        (ProcessRef, {Rem, false, Kill, Keep}) ->
            %% Check if grain activation is optimal.
            %% This won't cause a loop or an activation
            %% because is_location_right/2 is not an erleans_grain:call/2,3
            try erleans_grain:is_location_right(GrainRef, ProcessRef) of
                true ->
                    %% this process will be selected
                    {Rem - 1, true, Kill, [ProcessRef|Keep]};
                false ->
                    %% mark to terminate as we still have at least
                    %% one more in the list to check
                    {Rem - 1, false, [ProcessRef|Kill], [ProcessRef|Keep]};
                noproc ->
                    %% grain activation deactivated, or
                    %% some other process terminated it
                    {Rem - 1, false, Kill, Keep}
            catch
                _:_ ->
                    {Rem - 1, false, Kill, Keep}
            end;

        (ProcessRef, {Rem, true, Kill, Keep}) ->
            %% we already have the one to keep, so we terminate the rest
            %% Notice: the target node will update this same record on plum_db,
            %% removing ProcessRef from the list
            {Rem - 1, true, [ProcessRef|Kill], [ProcessRef|Keep]}
    end,

    Acc0 = {length(ProcessRefs1), false, [], []},

    case lists:foldl(Fun, Acc0, ProcessRefs1) of
        {0, _, [], ProcessRefs0} ->
            ok;
        {0, _, Kill, Keep} ->

            %% First we update the global registry
            ok = plum_db:put(?PDB_PREFIX, GrainRef, lists:usort(Keep)),
            ok = terminate_grains(GrainRef, Kill)
    end.


%% @private
terminate_grains(GrainRef, ProcessRefs) ->
    %% _ = [
    %%     begin
    %%         _ = partisan_gen_supervisor:terminate_child(
    %%             {erleans_grain_sup, partisan:node()}, ProcessRef
    %%         )
    %%     end || ProcessRef <- ProcessRefs
    %% ],

    %% spawn a terminator on the same node where the grain activation is
    _ = [
        begin
            %% TODO check if we can use a
            %% partisan_gen_server:cast({?MODULE, Node})
            Node = partisan:node(ProcessRef),
            partisan:spawn(Node, ?MODULE, terminator, [GrainRef, ProcessRef])
        end || ProcessRef <- ProcessRefs
    ],

    ok.



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
pick_first_alive([]) ->
    undefined;

pick_first_alive([ProcessRef | Rest]) ->
    try is_proc_alive(ProcessRef) of
        true ->
            ProcessRef;
        false ->
            pick_first_alive(Rest)
    catch
        error:_ ->
            pick_first_alive(Rest)
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
%% @doc Register the calling process with GrainRef unless another local
%% registration exists.
%% The call
%% @end
%% -----------------------------------------------------------------------------
-spec local_add(GrainRef :: erleans:grain_ref(), Pid :: pid()) ->
    ok | {error, {already_in_use, partisan_remote_ref:p()}}.

local_add(GrainRef, Pid) ->
    case ets:lookup(?TAB, GrainRef) of
        [] ->
            Ref = erlang:monitor(process, Pid),
            Objects = [{Pid, GrainRef, Ref}, {GrainRef, Pid, Ref}],
            true = ets:insert(?TAB, Objects),
            ok;
        [{OtherPid, GrainRef, _}] ->
            {error, {already_in_use, partisan_remote_ref:from_term(OtherPid)}}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_monitored(Pid :: pid()) -> boolean().

is_monitored(ProcessRef) ->
    Pid = partisan_remote_ref:to_term(ProcessRef),

    case ets:lookup(?TAB, Pid) of
        [{Pid, _, _}] -> true;
        [] -> false
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec local_remove(GrainRef :: erleans:grain_ref(), Pid :: pid()) -> ok.

local_remove(#{id := _} = GrainRef, Pid) when is_pid(Pid) ->
    true = ets:delete(?TAB, Pid),
    true = ets:delete(?TAB, GrainRef),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc Adds a locally registered reference to the distributed registry.
%% If the distributed registry contains other local references, it considers
%% them to be stale and removes them while adding the new one.
%% @end
%% -----------------------------------------------------------------------------
-spec global_add(GrainRef :: erleans:grain_ref(), Pid :: pid()) -> ok.


global_add(#{id := _} = GrainRef, Pid) ->
    New = [partisan_remote_ref:from_term(Pid)],

    case global_lookup(GrainRef) of
        undefined ->
            plum_db:put(?PDB_PREFIX, GrainRef, New);

        [] ->
            plum_db:put(?PDB_PREFIX, GrainRef, New);

        L0 when is_list(L0) ->
            %% We have multiple instanciations of this Grain or we have a
            %% temporal inconsistency, we add the new grain process reference
            %% removing all other local references (inconsistencies that occur
            %% if this node failed before and we it was not able to cleanup the
            %% distributed registry).
            L = lists:usort(exclude_local(L0) ++ New),
            plum_db:put(?PDB_PREFIX, GrainRef, L)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% %% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec global_lookup(GrainRef :: erleans:grain_ref()) ->
    [partisan_remote_ref:p()] | undefined.

global_lookup(#{id := _} = GrainRef) ->
    Opts = [
        {resolver, fun exclude_dead_resolver/2},
        {allow_put, false}
    ],
    global_lookup(GrainRef, Opts).


%% -----------------------------------------------------------------------------
%% @private
%% %% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec global_lookup(GrainRef :: erleans:grain_ref(), Opts :: map()) ->
    [partisan_remote_ref:p()] | undefined.

global_lookup(#{id := _} = GrainRef, Opts) ->
    case plum_db:get(?PDB_PREFIX, GrainRef, Opts) of
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
-spec global_remove(
    GrainRef :: erleans:grain_ref(),
    Pid :: pid() | partisan_remote_ref:p()) -> ok.

global_remove(#{id := _} = GrainRef, Term) ->
    case global_lookup(GrainRef) of
        undefined ->
            ok;
        [] ->
            ok;
        L when is_list(L) ->
            do_global_remove(GrainRef, L, Term)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
do_global_remove(GrainRef, L, Pid) when is_pid(Pid) ->
    do_global_remove(GrainRef, L, partisan_remote_ref:from_term(Pid));

do_global_remove(GrainRef, L0, ProcessRef) ->
    case L0 -- [ProcessRef] of
        [] ->
            ok = plum_db:delete(?PDB_PREFIX, GrainRef);
        L1 ->
            ok = plum_db:put(?PDB_PREFIX, GrainRef, lists:usort(L1))
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
resolver(A, B) ->
    resolver(A, B, fun(X) -> X end).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
resolver(?TOMBSTONE, ?TOMBSTONE, _) ->
    ?TOMBSTONE;

resolver(?TOMBSTONE, L, Fun) when is_list(L), is_function(Fun, 1) ->
    maybe_tombstone(Fun(L));

resolver(L, ?TOMBSTONE, Fun) when is_list(L), is_function(Fun, 1) ->
    maybe_tombstone(Fun(L));

resolver(L1, L2, Fun) when is_list(L1), is_list(L2), is_function(Fun, 1) ->
    %% Lists are sorted already as we sort them every time we do a put
    maybe_tombstone(Fun(lists:umerge(L1, L2))).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
exclude_dead_resolver(A, B) ->
    resolver(A, B, fun exclude_dead/1).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
exclude_unreachable_resolver(A, B) ->
    resolver(A, B, fun exclude_unreachable/1).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
exclude_local_resolver(A, B) ->
    resolver(A, B, fun exclude_local/1).


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
%% @doc Returns a new list where all the process references that are local
%% have been removed.
%% @end
%% -----------------------------------------------------------------------------
exclude_local(undefined) ->
    [];

exclude_local(ProcessRefs) when is_list(ProcessRefs) ->
    lists:filter(
        fun(ProcessRef) ->
            try
                not partisan:is_local(ProcessRef)
            catch
                _:_ ->
                    %% A remote process ref in a node we are not connected to
                    true
            end
        end,
        ProcessRefs
    ).


%% -----------------------------------------------------------------------------
%% @private
%% @doc Returns a new list where all the process references that are certain to
%% be dead have been removed.
%% A process is certain to be dead when {@link partisan:is_process_alive}
%% returns `false', or when is local and we do not have a local entry for this
%% process (the case for a previous instance of the same pid).
%% If the call to {@link partisan:is_process_alive} fails e.g. when we are not
%% connected to the node, it assumes the process is alive.
%% @end
%% -----------------------------------------------------------------------------
exclude_dead(undefined) ->
    [];

exclude_dead(ProcessRefs) when is_list(ProcessRefs) ->
    lists:filter(
        fun(ProcessRef) ->
            try
                is_proc_alive(ProcessRef)
            catch
                error:not_yet_connected ->
                    %% It might be alive, but we're not connected to the node
                    true;

                error:disconnected ->
                    %% It might be alive, but we're not connected to the node
                    true;

                _:_ ->
                    %% Unknwown
                    false
            end
        end,
        ProcessRefs
    ).


%% -----------------------------------------------------------------------------
%% @private
%% @doc Returns a new list where all the process references are know to be
%% reachable. A process is reachable if the process is local (and alive
%% according to the existance of a monitor) or is remote and
%% {@link partisan:is_process_alive/1} returns `true' for that process.
%% @end
%% -----------------------------------------------------------------------------
exclude_unreachable(undefined) ->
    [];

exclude_unreachable(ProcessRefs) when is_list(ProcessRefs) ->
    lists:filter(
        fun(ProcessRef) ->
            try
                is_proc_alive(ProcessRef)
            catch
                _:_ ->
                    false
            end
        end,
        ProcessRefs
    ).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_proc_alive(partisan_remote_ref:p()) -> boolean() | no_return().

is_proc_alive(ProcessRef) ->
    IsLocal = partisan:is_local(ProcessRef),

    (IsLocal andalso is_monitored(ProcessRef))
    orelse (not IsLocal andalso partisan:is_process_alive(ProcessRef)).


%% -----------------------------------------------------------------------------
%% @doc Unregisters all local alive processes.
%% @end
%% -----------------------------------------------------------------------------
-spec unregister_all() -> ok.

unregister_all() ->
    true = ets:safe_fixtable(?TAB, true),
    unregister_all(ets:first(?TAB)).

unregister_all(Pid) when is_pid(Pid) ->
    GrainRef = ets:lookup_element(?TAB, Pid, 2),
    ok = global_remove(GrainRef, Pid),
    true = local_remove(GrainRef, Pid),
    unregister_all(ets:next(?TAB, Pid));

unregister_all(#{id := _} = GrainRef) ->
    %% Ignore as we have two entries per registration
    %% {Pid, GrainRef} and {GrainRef, Pid}, we just use the first
    unregister_all(ets:next(?TAB, GrainRef));

unregister_all('$end_of_table') ->
    true = ets:safe_fixtable(?TAB, false),
    ok.


