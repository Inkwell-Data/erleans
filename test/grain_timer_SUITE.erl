%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% ---------------------------------------------------------------------------
-module(grain_timer_SUITE).

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         single_timer/1,
         multiple_timers/1,
         crashy_timer/1,
         stray_timer_message/1,
         recover_with_one_shots/1,
         recover_unfired_one_shot/1,
         completed_one_shots_removed/1,
         timer_requests_during_deactivation/1,
         timer_shutdown/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("erleans_timer.hrl").
-include("test_utils.hrl").

-define(g, timer_test_grain).

all() ->
    [{group, defaults},
     {group, deactivate_after_30}].

groups() ->
    [{defaults, [], [single_timer, multiple_timers, crashy_timer, stray_timer_message,
                    recover_with_one_shots, recover_unfired_one_shot,
                    completed_one_shots_removed, timer_requests_during_deactivation]},
     {deactivate_after_30, [], [timer_shutdown]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(defaults, Config) ->
    application:load(erleans),
    {ok, _} = application:ensure_all_started(erleans),
    Config;
init_per_group(deactivate_after_30, Config) ->
    application:load(erleans),
    application:set_env(erleans, deactivate_after, 30),
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_group(_, _Config) ->
    application:stop(erleans),
    application:unload(erleans),
    ok.

single_timer(_Config) ->
    Grain = erleans:get_grain(?g, <<"timer-test-grain">>),
    ?g:start_one_timer(Grain),
    timer:sleep(50),
    {ok, Acc} = ?g:clear(Grain),
    ?assertEqual([a, a, a, a, a], Acc),
    timer:sleep(50),
    ?g:cancel_one_timer(Grain),
    timer:sleep(50),
    {ok, Acc1} = ?g:clear(Grain),
    ?assertEqual([a, a, a, a, a], Acc1),
    ?g:start_one_timer(Grain),
    timer:sleep(50), % acc should be [a, a, a, a, a]
    Pid = case erleans_grain_registry:whereis_name(Grain) of
              GrainPid when is_pid(GrainPid) -> GrainPid
          end,
    ok = ?g:stop(Grain), % but should clear when it stops
    (fun Loop() ->
             case is_process_alive(Pid) of
                 false -> ok;
                 _ -> timer:sleep(20), Loop()
             end
     end)(),
    ?assertMatch({ok, _Node}, ?g:node(Grain)),  % reactivate the grain
    ?assertNotEqual(Pid, erleans_grain_registry:whereis_name(Grain)),
    ?assertEqual({ok, []}, ?g:clear(Grain)),
    ok.

multiple_timers(_Config) ->
    Grain = erleans:get_grain(timer_test_grain, <<"multiple-timer-test-grain">>),
    ?g:start_timers(Grain),
    timer:sleep(40),
    {ok, Acc} = ?g:clear(Grain),
    ?assertEqual([b, a, c, b, c, b, c, b],
                 lists:reverse(Acc)),
    timer:sleep(42),
    {ok, Acc1} = ?g:clear(Grain),
    ?assertEqual([c, b, c, b, c, b, c, b, c], % make sure a doesn't recur
                 lists:reverse(Acc1)),
    ?g:cancel_timers(Grain),
    timer:sleep(40),
    {ok, Acc2} = ?g:clear(Grain),
    ?assertEqual([], Acc2),
    ok.


crashy_timer(_Config) ->
    Grain = erleans:get_grain(timer_test_grain, <<"crashy-timer-test-grain">>),
    ?g:crashy_timer(Grain),
    timer:sleep(50),
    {ok, Acc} = ?g:clear(Grain),
    ?assertEqual([a, a, a, a, a, {erleans_timer_error,exit,boom}],
                 lists:reverse(Acc)),
    ok.

stray_timer_message(_Config) ->
    Grain = erleans:get_grain(?g, <<"stray-timer-message">>),
    Callback = fun(_, _) -> ok end,
    {ok, TimerPid} = erleans_grain:call(Grain, {start_timer, Callback, 60000, 1000}),
    true = is_pid(TimerPid),
    GrainPid = erleans_grain_registry:whereis_name(Grain),
    Monitor = monitor(process, TimerPid),
    TimerPid ! stray,
    receive {'DOWN', Monitor, process, TimerPid, normal} -> ok
    after 1000 -> ct:fail(timer_not_stopped)
    end,
    ?assertEqual({ok, [{erleans_timer_unexpected_msg, stray}]}, ?g:clear(GrainPid)),
    ?assertEqual(GrainPid, erleans_grain_registry:whereis_name(Grain)),
    ?assertEqual({ok, node()}, ?g:node(GrainPid)).

recover_with_one_shots(_Config) ->
    Grain = erleans:get_grain(?g, <<"recover-with-one-shots">>),
    Test = self(),
    Completed = fun(_, _) -> Test ! completed end,
    {ok, CompletedPid} = erleans_grain:call(Grain, {start_timer, Completed, 0, never}),
    true = is_pid(CompletedPid),
    CompletedMonitor = monitor(process, CompletedPid),
    receive completed -> ok after 1000 -> ct:fail(one_shot_not_called) end,
    receive {'DOWN', CompletedMonitor, process, CompletedPid, _} -> ok
    after 1000 -> ct:fail(one_shot_not_finished)
    end,

    InFlight = fun(_, _) ->
                       Test ! {in_flight, self()},
                       receive finish -> ok end
               end,
    {ok, InFlightPid} = erleans_grain:call(Grain, {start_timer, InFlight, 0, never}),
    true = is_pid(InFlightPid),
    InFlightMonitor = monitor(process, InFlightPid),
    receive {in_flight, InFlightPid} -> ok after 1000 -> ct:fail(one_shot_not_started) end,
    Periodic = fun(_, _) -> Test ! periodic end,
    {ok, _} = erleans_grain:call(Grain, {start_timer, Periodic, 60000, 10}),

    Pid = erleans_grain_registry:whereis_name(Grain),
    ok = ?g:stop(Pid),
    ?assertMatch({deactivating, _}, sys:get_state(Pid)),
    ?assertEqual({ok, node()}, ?g:node(Pid)),
    receive periodic -> ok after 1000 -> ct:fail(periodic_timer_not_recovered) end,
    InFlightPid ! finish,
    receive {'DOWN', InFlightMonitor, process, InFlightPid, normal} -> ok
    after 1000 -> ct:fail(one_shot_not_finished)
    end,
    receive
        completed -> ct:fail(completed_one_shot_restarted);
        {in_flight, _} -> ct:fail(in_flight_one_shot_restarted)
    after 50 -> ok
    end,
    ?assertEqual(Pid, erleans_grain_registry:whereis_name(Grain)),
    ?assertEqual({ok, node()}, ?g:node(Pid)),
    ok = ?g:cancel_one_timer(Pid).

completed_one_shots_removed(_) ->
    Grain = erleans:get_grain(?g, <<"completed-one-shots-removed">>),
    Test = self(),
    Tag = make_ref(),
    Callback = fun(_, _) -> Test ! {Tag, completed} end,
    [erleans_grain:call(Grain, {start_timer, Callback, 0, never})
     || _ <- lists:seq(1, 100)],
    [receive {Tag, completed} -> ok after 1000 -> ct:fail(timer_not_fired) end
     || _ <- lists:seq(1, 100)],
    ?UNTIL(erleans_grain:call(Grain, timer_pids) =:= []),
    %% Internal cleanup notifications must not reach the grain's handle_info.
    ?assertEqual({ok, []}, ?g:clear(Grain)).

recover_unfired_one_shot(_) ->
    Grain = erleans:get_grain(?g, <<"recover-unfired-one-shot">>),
    Test = self(),
    Tag = make_ref(),
    Blocker = fun(_, _) ->
        Test ! {Tag, blocked, self()},
        receive finish -> ok end
    end,
    {ok, BlockerPid} = erleans_grain:call(Grain, {start_timer, Blocker, 0, never}),
    true = is_pid(BlockerPid),
    receive {Tag, blocked, BlockerPid} -> ok after 1000 -> ct:fail(timer_not_started) end,
    BlockerMonitor = monitor(process, BlockerPid),
    Callback = fun(_, _) -> Test ! {Tag, fired, self(), erlang:monotonic_time(millisecond)} end,
    Start = erlang:monotonic_time(millisecond),
    {ok, Pending} = erleans_grain:call(Grain, {start_timer, Callback, 300, never}),
    true = is_pid(Pending),
    %% Force an old tick to be ahead of cancellation, but not yet claimed.
    true = erlang:suspend_process(Pending),
    try
        Pending ! ?tick,
        Pid = erleans_grain_registry:whereis_name(Grain),
        true = is_pid(Pid),
        ok = ?g:stop(Pid),
        ?assertMatch({deactivating, _}, sys:get_state(Pid)),
        ?assertEqual({ok, node()}, ?g:node(Pid)),
        true = erlang:resume_process(Pending),
        %% The old timer must not invoke its callback after recovery replaced it.
        Fired = receive {Tag, fired, FiredPid, Time} ->
                    ?assertNotEqual(Pending, FiredPid),
                    ?assert(Time >= Start + 300),
                    FiredPid
                after 1000 -> ct:fail(unfired_timer_not_recovered)
                end,
        FiredMonitor = monitor(process, Fired),
        receive {'DOWN', FiredMonitor, process, Fired, _} -> ok
        after 1000 -> ct:fail(one_shot_not_finished)
        end,
        ?UNTIL(erleans_grain:call(Pid, timer_pids) =:= [BlockerPid]),
        %% Revival must keep tracking callbacks still running on the old timer.
        GrainMonitor = monitor(process, Pid),
        ok = ?g:stop(Pid),
        ?assertMatch({deactivating, _}, sys:get_state(Pid)),
        BlockerPid ! finish,
        receive {'DOWN', BlockerMonitor, process, BlockerPid, normal} -> ok
        after 1000 -> ct:fail(blocker_not_finished)
        end,
        receive {'DOWN', GrainMonitor, process, Pid, {shutdown, deactivated}} -> ok
        after 1000 -> ct:fail(grain_not_deactivated)
        end,
        receive {Tag, fired, _, _} -> ct:fail(one_shot_repeated)
        after 0 -> ok
        end
    after
        try erlang:resume_process(Pending) catch error:badarg -> ok end,
        BlockerPid ! finish
    end.

timer_requests_during_deactivation(_Config) ->
    Grain = erleans:get_grain(?g, <<"timer-requests-during-deactivation">>),
    Test = self(),
    Callback = fun(Ref, _) ->
                       Test ! {timer_started, self()},
                       receive call_grain -> ok end,
                       ok = ?g:accumulate(Ref, call),
                       ok = erleans_grain:cast(Ref, {accumulate, cast}),
                       Test ! {timer_reply, self(), ?g:clear(Ref)},
                       receive finish -> ok end
               end,
    {ok, TimerPid} = erleans_grain:call(Grain, {start_timer, Callback, 0, never}),
    true = is_pid(TimerPid),
    receive {timer_started, TimerPid} -> ok
    after 1000 -> ct:fail(timer_not_started)
    end,
    Pid = erleans_grain_registry:whereis_name(Grain),
    GrainMonitor = monitor(process, Pid),
    TimerMonitor = monitor(process, TimerPid),
    ok = ?g:stop(Pid),
    ?assertMatch({deactivating, _}, sys:get_state(Pid)),
    TimerPid ! call_grain,
    receive {timer_reply, TimerPid, Reply} -> ?assertEqual({ok, [cast, call]}, Reply)
    after 1000 -> ct:fail(timer_call_not_replied)
    end,
    ?assertMatch({deactivating, _}, sys:get_state(Pid)),
    TimerPid ! finish,
    receive {'DOWN', TimerMonitor, process, TimerPid, normal} -> ok
    after 1000 -> ct:fail(timer_not_finished)
    end,
    receive {'DOWN', GrainMonitor, process, Pid, {shutdown, deactivated}} -> ok
    after 1000 -> ct:fail(grain_not_deactivated)
    end.

timer_shutdown(_Config) ->
    Grain = erleans:get_grain(timer_test_grain, <<"shutdown-timer-test-grain">>),

    ?g:long_timer(Grain),

    timer:sleep(80),  % sleep till we should be way shut down, but timer should keep us awake

    %% recover
    ?g:node(Grain),
    timer:sleep(15),

    {ok, Acc0} = ?g:clear(Grain),
    Acc = lists:reverse(Acc0),

    {Calls, Pids} = lists:unzip(Acc),
    %% two short before that timer gets cancelled, then a long and a
    %% short from the restarted timer
    ?assertEqual([short, short, long, short], Calls),
    %% we should have 3 pids here, two for the short, one for the
    %% long.  If we waited long enough for another long, there would
    %% be four
    ?assertEqual(3, length(lists:usort(Pids))),

    ok.
