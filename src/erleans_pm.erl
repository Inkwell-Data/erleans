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

-export([register_name/2,
         unregister_name/1,
         unregister_name/2,
         whereis_name/1,
         add/2,
         remove/2,
         terminate_duplicates/2,
         terminator/2,
         send/2]).

-include("erleans.hrl").
-include_lib("kernel/include/logger.hrl").

-dialyzer({nowarn_function, register_name/2}).
-dialyzer({nowarn_function, unregister_name/2}).

-define(TOMBSTONE, '$deleted').

-spec register_name(Name :: term(), Pid :: pid()) -> yes | no.
register_name(GrainRef, Pid) when is_pid(Pid) ->
    case add(GrainRef, Pid) of
        ok -> yes;
        _ -> no
    end.

-spec unregister_name(Name :: term()) -> Name :: term() | fail.
unregister_name(Name) ->
    case ?MODULE:whereis_name(Name) of
        Pid when is_pid(Pid) ->
            unregister_name(Name, Pid);
        undefined ->
            undefined
    end.

-spec unregister_name(Name :: term(), Pid :: pid()) -> Name :: term() | fail.
unregister_name(GrainRef, Pid) ->
    case remove(GrainRef, Pid) of
        ok ->
            GrainRef;
        _ ->
            fail
    end.

%% @private
terminate_duplicates(GrainRef, List) ->
    Fun = fun(Pid, {1, undefined}) ->
                %% keep the last one as non of them replied true
                {0, Pid};
             (Pid, {Rem, undefined}) ->
                %% checks if grain activation is co-located with
                %% the Kafka partition, won't cause a loop or an activation
                %% because below call is not an erleans_grain:call/2,3
                case twin_service_grain:is_location_right(Pid) of
                    true ->
                        Pid;
                    false ->
                        %% terminate the grain as we still have at least
                        %% one more in the list to check
                        terminate(GrainRef, Pid),
                        {Rem - 1, undefined};
                    noproc ->
                        %% grain activation deactivated, or
                        %% some other process terminated it
                        {Rem - 1, undefined}
                end;
             (Pid, {Rem, Keep}) ->
                %% we already have the one to keep, so we terminate the rest
                terminate(GrainRef, Pid),
                {Rem - 1, Keep}
          end,
    {_, Pid} = lists:foldl(Fun, {length(List), undefined}, List),
    Pid.

%% @private
terminate(GrainRef, Pid) ->
    %% spawn a terminator on the same node where the grain activation is
    spawn(node(Pid), ?MODULE, terminator, [GrainRef, Pid]).

-spec terminator(GrainRef :: erleans:grain_ref(), Pid :: pid()) -> ok.
terminator(GrainRef, Pid) ->
    supervisor:terminate_child({erleans_grain_sup, node()}, Pid),
    remove(GrainRef, Pid).

-spec whereis_name(GrainRef :: erleans:grain_ref()) -> pid() | undefined.
whereis_name(GrainRef=#{placement := stateless}) ->
    whereis_stateless(GrainRef);
whereis_name(GrainRef=#{placement := {stateless, _}}) ->
    whereis_stateless(GrainRef);
whereis_name(GrainRef) ->
    case gproc:where(?stateful(GrainRef)) of
        Pid when is_pid(Pid) ->
            Pid;
        _ ->
            case plum_db:get({?MODULE, grain_ref}, GrainRef, [{resolver, fun resolve/2}]) of
                Pids when is_list(Pids) ->
                    terminate_duplicates(GrainRef, Pids);
                undefined ->
                    undefined
            end
    end.

-spec send(Name :: term(), Message :: term()) -> pid().
send(Name, Message) ->
    case whereis_name(Name) of
        Pid when is_pid(Pid) ->
            Pid ! Message;
        undefined ->
            error({badarg, Name})
    end.

%% @private
whereis_stateless(GrainRef) ->
    case gproc_pool:pick_worker(GrainRef) of
        false ->
            undefined;
        Pid ->
            Pid
    end.

-spec add(GrainRef :: erleans:grain_ref(), Pid :: pid()) -> ok.
add(GrainRef, Pid) ->
    Fun =
    fun(undefined) ->
         [Pid];
       ([?TOMBSTONE]) ->
         [Pid];
       ([Pids]) ->
         lists:usort(Pids ++ [Pid]);
       (Vs) ->
         lists:usort(resolve(Vs) ++ [Pid])
    end,
    plum_db:put({?MODULE, grain_ref}, GrainRef, Fun).

-spec remove(GrainRef :: erleans:grain_ref(), Pid :: pid()) -> ok.
remove(GrainRef, Pid) ->
    Fun =
    fun([?TOMBSTONE]) ->
          ?TOMBSTONE;
       ([[P]]) when P == Pid ->
          ?TOMBSTONE;
       ([Pids]) ->
          Pids -- [Pid];
       (Vs) ->
          case resolve(Vs) of
              [Pid] ->
                  ?TOMBSTONE;
              Pids ->
                  Pids -- [Pid]
          end
    end,
    plum_db:put({?MODULE, grain_ref}, GrainRef, Fun).

%% @private
resolve(L1, L2) ->
    resolve([L1, L2]).

%% @private
resolve(L) ->
    lists:umerge(L).
