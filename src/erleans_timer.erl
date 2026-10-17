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
-module(erleans_timer).

-moduledoc """
Timers are simple periodic or aperiodic timers that will
invoke a callback when they fire.  While they do not have direct
access to grain state or identity, their fun can contain anything,
so they can easily send a message to a grain.

`cancel_timer/0` is supplied as a helper for simple situations
where a grain only has one timer going.  In the case that there
many timers going at once, `cancel_timer/1` must be used
explicitly on each.

A note about timers and shutdown.  Following Orleans, calls made
from timers do not extend the lease time on a grain and a long
call made in a timer callback can extend the life of the grain.
For that reason, timers can miss ticks and tick too quickly around
the lease time of a grain.
""".

-include("erleans_timer.hrl").

-export([start/3,
         start/4,
         cancel/0,
         cancel/1,
         recoverable_cancel/0,
         check/0,
         recover/0,
         finished/1]).

-record(timer,
        {
          grain :: pid(),
          grain_ref :: erleans:grain_ref() | undefined,
          callback :: function(),
          args :: term(),
          period :: pos_integer() | never,
          deadline :: integer(),
          fired :: reference() | undefined
        }).

-opaque timer() :: #timer{}.
-export_type([timer/0]).

start(Callback, Args, StartTime) ->
    start(Callback, Args, StartTime, never).

start(Callback, Args, StartTime, Period) ->
    case get(grain_ref) of
        undefined ->
            {error, called_outside_of_grain_context};
        GrainRef=#{} ->
            GrainPid = self(),
            Timer =
                #timer{grain = GrainPid,
                       grain_ref = GrainRef,
                       callback = Callback,
                       args = Args,
                       period = Period,
                       deadline = erlang:monotonic_time(millisecond) + StartTime,
                       fired = case Period of
                                   never -> atomics:new(1, []);
                                   _ -> undefined
                               end},
            Pid = start_timer(Timer#timer.deadline, Timer),
            case get(?key) of
                undefined ->
                    put(?key, #{Pid => Timer});
                Map ->
                    put(?key, Map#{Pid => Timer})
            end,
            {ok, Pid}
    end.

cancel() ->
    case get(?key) of
        undefined ->
            {error, no_timer_to_cancel};
        Map when is_map(Map) ->
            maps:fold(fun(Pid, _V, Acc) ->
                              Pid ! ?cancel,
                              Acc
                      end, beep, Map),
            erlang:erase(?key),
            ok
    end.

cancel(Pid) when is_pid(Pid) ->
    case get(?key) of
        #{Pid := _} = Map ->
            Pid ! ?cancel,
            put(?key, maps:remove(Pid, Map)),
            ok;
        _ ->
            {error, no_timer_to_cancel}
    end.

%%% in order to be able to recover the list of timers when the grain
%%% starts to go down and then is reactivated before all of the
%%% in-process timer ticks can complete, we need to have a slightly
%%% different form of cancel, which leaves the timer information
%%% structure intact so we can restart them if we need to.
recoverable_cancel() ->
    case get(?key) of
        undefined ->
            {error, no_timer_to_cancel};
        Map when is_map(Map) ->
            maps:fold(fun(Pid, _V, Acc) ->
                              Pid ! ?cancel,
                              Acc
                      end, beep, Map),
            erase(?key),
            put(?recovery, Map)
    end.

check() ->
    case get(?recovery) of
        undefined ->
            finished;
        Map when is_map(Map) ->
            AnythingAlive =
                maps:fold(fun(_Pid, _V, true) ->
                                  true;
                             (Pid, _V, _) ->
                                  is_process_alive(Pid)
                          end, false, Map),
            case AnythingAlive of
                false ->
                    finished;
                _ ->
                    pending
            end
    end.

recover() ->
    case get(?recovery) of
        undefined ->
            ok;
        Map when is_map(Map) ->
            NewMap = maps:fold(fun recover_timer/3, #{}, Map),
            erase(?recovery),
            put(?key, NewMap)
    end.


%% Internal completion notifications are consumed by the grain, not callbacks.
finished(Pid) ->
    lists:foreach(fun(Key) ->
        case get(Key) of
            Map when is_map(Map) -> put(Key, maps:remove(Pid, Map));
            _ -> ok
        end
    end, [?key, ?recovery]),
    ok.

recover_timer(Pid, #timer{period=never, fired=Fired, deadline=Deadline}=Timer, Acc) ->
    case atomics:compare_exchange(Fired, 1, 0, 2) of
        ok ->
            %% Retire the old timer before replacing it. If its tick already
            %% won the race, the in-flight branch below keeps tracking it.
            NewTimer = Timer#timer{fired=atomics:new(1, [])},
            NewPid = start_timer(Deadline, NewTimer),
            Acc#{NewPid => NewTimer};
        1 ->
            %% Keep tracking an in-flight callback so another deactivation
            %% still waits for it, but never start it again.
            case is_process_alive(Pid) of
                true -> Acc#{Pid => Timer};
                false -> Acc
            end;
        2 -> Acc
    end;
recover_timer(_Pid, #timer{period=Period}=Timer, Acc) ->
    NewPid = start_timer(erlang:monotonic_time(millisecond) + Period, Timer),
    Acc#{NewPid => Timer}.


%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal functions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

loop(FireTime, #timer{grain = Pid,
                      grain_ref = GrainRef,
                      callback = Callback,
                      args = Args,
                      period = Period} = Timer) ->
    receive
        ?cancel ->
            unlink(Pid),
            ok;
        ?tick ->
            case claim_tick(Timer) of
                false -> timer_finished(Pid);
                true ->
                    try Callback(GrainRef, Args) of
                        _ ->
                            case Period of
                                never ->
                                    timer_finished(Pid);
                                _ ->
                                    NextFire = FireTime + Period,
                                    erlang:send_after(NextFire, self(), ?tick,
                                                      [{abs, true}]),
                                    loop(NextFire, Timer)
                            end
                    catch Class:Error ->
                            Pid ! {erleans_timer_error, Class, Error},
                            timer_finished(Pid)
                    end
            end;
        Msg ->
            Pid ! {erleans_timer_unexpected_msg, Msg},
            timer_finished(Pid)
    end.

%% A first tick and recovery compete to claim or retire the old one-shot.
%% A retired timer cannot run its callback after a replacement has started.
claim_tick(#timer{period=never, fired=Fired}) ->
    atomics:compare_exchange(Fired, 1, 0, 1) =:= ok;
claim_tick(_) -> true.

timer_finished(Pid) ->
    unlink(Pid),
    Pid ! {erleans_timer_finished, self()},
    ok.

start_timer(FirstFire, Timer) ->
    spawn_link(fun() ->
                   %% requests to a grain from a timer callback
                   %% do not reset the activation expiry timer
                   put(req_type, leave_timer),
                   erlang:send_after(FirstFire, self(), ?tick,
                                     [{abs, true}]),
                   loop(FirstFire, Timer)
               end).
