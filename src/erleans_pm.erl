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
    case exclude_unreachable(global_lookup(GrainRef)) of
        [] ->
            %% We call monitor first as it acts as a memory barrier,
            %% serialising all local concurrent registrations.
            case local_add(GrainRef) of
                ok ->
                    global_add(GrainRef);

                {error, {already_in_use, _}} = Error ->
                    %% Some other process beat us in calling local_add
                    Error
            end;

        [ProcessRef|_] ->
            {error, {already_in_use, ProcessRef}}
    end.


%% -----------------------------------------------------------------------------
%% @doc It can only be called by the caller
%% @end
%% -----------------------------------------------------------------------------
-spec unregister_name(GrainRef :: erleans:grain_ref()) ->
    ok | no_return().

unregister_name(#{id := _} = GrainRef) ->
    Opts = [
        {resolver, fun resolver/2},
        {allow_put, true}
    ],
    case global_lookup(GrainRef, Opts) of
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
                    ok = global_remove(GrainRef, ProcessRef),
                    Pid = partisan_remote_ref:to_term(ProcessRef),
                    true = local_remove(GrainRef, Pid),
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
    _ = partisan_gen_supervisor:terminate_child({erleans_grain_sup, partisan:node()}, Pid),
    global_remove(GrainRef, ProcessRef).


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
    Opts = [
        {resolver, fun resolver/2},
        {allow_put, true}
    ],
    case global_lookup(GrainRef, Opts) of
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
    case ets:info(?TAB, name) of
        undefined ->
            Opts = [
                set,
                public,
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
            global_remove(GrainRef, Pid),
            ok = local_remove(GrainRef, Pid)
        end,
        ets:lookup(?TAB, Pid)
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

    Resolved = plum_db_object:resolve(Obj, fun resolver/2),

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
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec global_add(GrainRef :: erleans:grain_ref()) -> ok.

global_add(GrainRef) ->
    global_add(GrainRef, partisan:self()).


%% -----------------------------------------------------------------------------
%% @private
%% @doc Adds a locally registered reference to the distributed registry.
%% If the distributed registry contains other local references, it considers
%% them to be stale and removes them while adding the new one.
%% @end
%% -----------------------------------------------------------------------------
-spec global_add(GrainRef :: erleans:grain_ref(), partisan_remote_ref:p()) ->
    ok.

global_add(#{id := _} = GrainRef, ProcessRef) ->
    New = [ProcessRef],

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
        {resolver, fun resolver/2},
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
%% @doc Register the calling process with GrainRef unless another local
%% registration exists.
%% The call
%% @end
%% -----------------------------------------------------------------------------
-spec local_add(GrainRef :: erleans:grain_ref()) ->
    ok | {error, {already_in_use, partisan_remote_ref:p()}}.

local_add(GrainRef) ->
    Pid = self(),
    Objects = [{Pid, GrainRef}, {GrainRef, Pid}],

    %% We guarantee that locally it can only be one activation
    case ets:insert_new(?TAB, Objects) of
        true ->
            gen_server:cast(?MODULE, {monitor, Pid});
        false ->
            OtherPid = ets:lookup_element(?TAB, GrainRef, 2),
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
        [{Pid, _}] -> true;
        _ -> false
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec local_remove(GrainRef :: erleans:grain_ref(), Pid :: pid()) -> ok.

local_remove(#{id := _} = GrainRef, Pid) when is_pid(Pid) ->
    true = ets:delete_object(?TAB, {Pid, GrainRef}),
    true = ets:delete_object(?TAB, {GrainRef, Pid}),
    ok.



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
resolver(?TOMBSTONE, ?TOMBSTONE) ->
    ?TOMBSTONE;

resolver(?TOMBSTONE, L) when is_list(L) ->
    maybe_tombstone(exclude_dead(L));

resolver(L, ?TOMBSTONE) when is_list(L) ->
    maybe_tombstone(exclude_dead(L));

resolver(L1, L2) when is_list(L1) andalso is_list(L2) ->
    %% Lists are sorted already as we sort them every time we do a put
    maybe_tombstone(exclude_dead(lists:umerge(L1, L2))).


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


is_proc_alive(ProcessRef) ->
    IsLocal = partisan:is_local(ProcessRef),

    (IsLocal andalso is_monitored(ProcessRef))
    orelse
    (not IsLocal andalso partisan:is_process_alive(ProcessRef)).


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
                not partisan:is_local_pid(ProcessRef)
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
%% @doc Returns a new list where all the process references are know to be
%% exclude_unreachable. A process is reachable if the function
%% {@link partisan:is_process_alive} returns `true' for that process.
%% @end
%% -----------------------------------------------------------------------------
exclude_unreachable(undefined) ->
    [];

exclude_unreachable(ProcessRefs) when is_list(ProcessRefs) ->
    lists:filter(
        fun(ProcessRef) ->
            try
                partisan:is_process_alive(ProcessRef)
            catch
                _:_ ->
                    false
            end
        end,
        ProcessRefs
    ).



%% -----------------------------------------------------------------------------
%% @doc Unregisters all local alive processes.
%% @end
%% -----------------------------------------------------------------------------
-spec unregister_all() -> ok.

unregister_all() ->
    true = ets:safe_fixtable(?TAB, true),
    unregister_all(ets:first(?TAB)).

unregister_all(Pid) when is_pid(Pid) ->
    [GrainRef] = ets:lookup_element(?TAB, Pid, 2),
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


