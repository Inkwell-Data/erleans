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
-export([start_link/0,
         register_name/2,
         unregister_name/1,
         unregister_name/2,
         whereis_name/1]).

%% called by this module via spawning
-export([terminator/2]).

%% only for manual testing or OAM
-export([plum_db_add/2,
         plum_db_remove/2,
         plum_db_get/1]).

%% gen_server callbacks to handle plum_db events
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-include("erleans.hrl").
-include_lib("kernel/include/logger.hrl").

-dialyzer({nowarn_function, register_name/2}).
-dialyzer({nowarn_function, unregister_name/2}).

-define(TOMBSTONE, '$deleted').



-spec register_name(GrainRef :: erleans:grain_ref(), Pid :: pid()) -> yes | no.

register_name(GrainRef, Pid) when is_pid(Pid) ->
    case plum_db_add(GrainRef, Pid) of
        ok ->
            erleans_monitor:monitor(GrainRef, Pid),
            yes;
        _ ->
            no
    end.

-spec unregister_name(GrainRef :: erleans:grain_ref())
        -> GrainRef :: erleans:grain_ref() | fail.
unregister_name(GrainRef) ->
    case ?MODULE:whereis_name(GrainRef) of
        Pid when is_pid(Pid) ->
            unregister_name(GrainRef, Pid);
        undefined ->
            undefined
    end.

-spec unregister_name(GrainRef :: erleans:grain_ref(), Pid :: pid())
        -> GrainRef :: erleans:grain_ref() | fail.
unregister_name(GrainRef, Pid) ->
    case plum_db_remove(GrainRef, Pid) of
        ok ->
            erleans_monitor:demonitor(GrainRef, Pid),
            GrainRef;
        _ ->
            fail
    end.

%% @private
terminate_duplicates(GrainRef, Pids) ->
    Fun = fun(Pid, {1, undefined}) ->
                %% keep the last one as non of them replied true before
                {0, Pid};
             (Pid, {Rem, undefined}) ->
                %% checks if grain activation is co-located with
                %% the Kafka partition, won't cause a loop or an activation
                %% because below call is not an erleans_grain:call/2,3
                case erleans_grain:is_location_right(GrainRef, Pid) of
                    true ->
                        %% this process will be selected
                        {Rem - 1, Pid};
                    false ->
                        %% terminate the grain as we still have at least
                        %% one more in the list to check
                        terminate_grain(GrainRef, Pid),
                        {Rem - 1, undefined};
                    noproc ->
                        %% grain activation deactivated, or
                        %% some other process terminated it
                        plum_db_remove(GrainRef, Pid),
                        {Rem - 1, undefined}
                end;
             (Pid, {Rem, Keep}) ->
                %% we already have the one to keep, so we terminate the rest
                terminate_grain(GrainRef, Pid),
                {Rem - 1, Keep}
          end,
    {_, Pid} = lists:foldl(Fun, {length(Pids), undefined}, Pids),
    Pid.

%% @private
terminate_grain(GrainRef, Pid) ->
    %% spawn a terminator on the same node where the grain activation is
    spawn(partisan:node(Pid), ?MODULE, terminator, [GrainRef, Pid]).

-spec terminator(GrainRef :: erleans:grain_ref(), Pid :: pid())
        -> ok.
terminator(GrainRef, Pid) ->
    supervisor:terminate_child({erleans_grain_sup, partisan:node()}, Pid),
    plum_db_remove(GrainRef, Pid).

-spec whereis_name(GrainRef :: erleans:grain_ref()) ->
    partisan_remote_ref:p() | undefined.

whereis_name(GrainRef=#{placement := stateless}) ->
    whereis_stateless(GrainRef);
whereis_name(GrainRef=#{placement := {stateless, _}}) ->
    whereis_stateless(GrainRef);
whereis_name(GrainRef) ->
    case gproc_lookup(GrainRef) of
        Pid when is_pid(Pid) ->
            %% We do this so that we have only encoded pids as return values
            %% Since we are not using distributed GPROC, this Pid is local
            partisan_remote_ref:from_term(Pid);
        undefined ->
            case plum_db_get(GrainRef) of
                Pids when is_list(Pids) ->
                    pick_first_alive(Pids);
                undefined ->
                    undefined
            end
    end.

%% @private
gproc_lookup(GrainRef) ->
    case gproc:where(?stateful(GrainRef)) of
        Pid when is_pid(Pid) ->
            case erlang:is_process_alive(Pid) of
                true ->
                    Pid;
                false ->
                    undefined
            end;
        _ ->
            undefined
    end.

%% @private
pick_first_alive([]) ->
    undefined;
pick_first_alive([Pid | Pids]) ->
    case is_proc_alive(Pid) of
        true ->
            Pid;
        false ->
            pick_first_alive(Pids)
    end.

%% @private
whereis_stateless(GrainRef) ->
    case gproc_pool:pick_worker(GrainRef) of
        false ->
            undefined;
        Pid ->
            Pid
    end.




-spec plum_db_add(GrainRef :: erleans:grain_ref(), Pid :: pid()) -> ok.

plum_db_add(GrainRef, Pid) ->
    EncodedPid = partisan_remote_ref:from_term(Pid),
    PKey = {?MODULE, grain_ref},
    Opts = [
        {resolver, fun resolver/2},
        {allow_put, false} % We will manually put as we need to add Pid
    ],

    case plum_db:get(PKey, GrainRef, Opts) of
        undefined ->
            plum_db:put(PKey, GrainRef, [EncodedPid]);
        [] ->
            plum_db:put(PKey, GrainRef, [EncodedPid]);
        L when is_list(L) ->
            plum_db:put(PKey, GrainRef, lists:usort(L ++ [EncodedPid]))
    end.



-spec plum_db_remove(GrainRef :: erleans:grain_ref(), Pid :: pid()) -> ok.

plum_db_remove(GrainRef, Pid) ->
    EncodedPid = partisan_remote_ref:from_term(Pid),
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
        L0 when is_list(L0) ->
            case L0 -- [EncodedPid] of
                [] ->
                    plum_db:delete(PKey, GrainRef);
                L1 ->
                    plum_db:put(PKey, GrainRef, lists:usort(L1))
            end
    end.


-spec plum_db_get(GrainRef :: erleans:grain_ref()) ->
    list(partisan_remote_ref:p()) | undefined.

plum_db_get(GrainRef) ->
    Opts = [
        {resolver, fun resolver/2},
        {allow_put, true} % This will make a put to store the resolved value
    ],
    plum_db:get({?MODULE, grain_ref}, GrainRef, Opts).



%% @private
resolver(?TOMBSTONE, ?TOMBSTONE) ->
    ?TOMBSTONE;
resolver(?TOMBSTONE, L) ->
    maybe_tombstone(remove_dead(L));
resolver(L, ?TOMBSTONE) ->
    maybe_tombstone(remove_dead(L));
resolver(L1, L2) when is_list(L1) andalso is_list(L2) ->
    %% Lists are sorted already as we sort them every time we put
    maybe_tombstone(
        remove_dead(lists:umerge(L1, L2))
    ).

%% @private
remove_dead(Pids) ->
    lists:filter(
        fun(Pid) -> is_proc_alive(Pid) end, Pids
    ).

is_proc_alive(EncodedPid) ->
    case partisan:is_local_pid(EncodedPid) of
        true ->
            partisan:is_process_alive(EncodedPid);
        false ->
            Node = partisan:node(EncodedPid),
            Result = partisan_rpc:call(
                Node, partisan, is_process_alive, [EncodedPid], 5000
            ),
            case Result of
                {badrpc, _Reason} ->
                    %% Reason can be nodedown, timeout, etc.
                    false;
                Result ->
                    Result
            end
    end.


%% @private
maybe_tombstone([]) ->
    ?TOMBSTONE;

maybe_tombstone(L) ->
    L.

start_link() ->
    partisan_gen_server:start_link({local, ?MODULE}, ?MODULE, _Args = #{}, []).



%% =============================================================================
%% GEN_SERVER BEHAVIOR CALLBACKS
%% =============================================================================

-spec init(Args :: term()) ->
    {ok, State :: term()}.

init(#{} = _Args) ->
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

handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(Message :: term(), State :: term()) ->
    {noreply, NewState :: term()}.

handle_info(
    {plum_db_event, object_update, {{{_, _}, GrainRef}, Obj, PrevObj}},
    State) ->
    logger:debug(#{
        message => "plum_db_event object_update received",
        object => Obj,
        prev_obj => PrevObj
    }),

    case plum_db_object:value(plum_db_object:resolve(Obj, lww)) of
        EncodedPids when is_list(EncodedPids), length(EncodedPids) > 1 ->
            %% we need to remove dead processes first to avoid the case when we
            %% kill one that returns false in the belief that there's at least
            %% one remaining but that could actually be dead already
            PidsAlive = remove_dead(EncodedPids),
            _Pid = terminate_duplicates(GrainRef, PidsAlive);
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

