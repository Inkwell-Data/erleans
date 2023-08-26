-module(erleans_pm_SUITE).

-define(DEACTIVATE_AFTER, 5).
-define(SLEEP, timer:sleep(?DEACTIVATE_AFTER + 1)).
-define(PDB_PREFIX, {erleans_pm, grain_ref}).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("test_utils.hrl").


all() ->
     [
        {group, main}
    ].

groups() ->
    [
        {main, [], [
            register_name,
            whereis_name,
            already_in_use,
            stale_local_entry,
            unreachable_remote_entry
        ]}
    ].


init_per_group(_, Config) ->
    application:load(plum_db), % will load partisan
    application:load(erleans),
    application:set_env(erleans, deactivate_after, ?DEACTIVATE_AFTER),
    {ok, _} = application:ensure_all_started(plum_db),
    {ok, _} = application:ensure_all_started(erleans),

    GrainRef = erleans:get_grain(test_grain, <<"grain1">>),
    [{grainref, GrainRef}|Config].


end_per_group(_, _Config) ->
    application:stop(erleans),
    application:stop(plum_db),
    application:unload(erleans),
    application:unload(plum_db),
    ok.


init_per_suite(Config) ->
    dbg:stop(),
    Config.


end_per_suite(_Config) ->
    ok.


init_per_testcase(_, Config) ->
    Config.


end_per_testcase(_, _Config) ->
    ok.




%% =============================================================================
%% CASES
%% =============================================================================



register_name(_) ->
    ?assertMatch(
        {error, badgrain},
        erleans_pm:register_name(),
        "I (caller) am not a grain, so register should be disallowed"
    ).


whereis_name(Config) ->
    GrainRef = ?config(grainref, Config),
    {ok, 0} = test_grain:call_counter(GrainRef),
    %% Grain should have beend activated and registered

    PidRef = erleans_pm:whereis_name(GrainRef),
    Node = partisan:node(),
    ?assertMatch([Node|_PidStr], PidRef),

    ?SLEEP,

    ?assertMatch(undefined, erleans_pm:whereis_name(GrainRef)).


already_in_use(Config) ->
    GrainRef = ?config(grainref, Config),

    {ok, 1} = test_grain:call_counter(GrainRef),
    %% Grain should have beend activated and registered

    %% We simulate a local duplicate registration
    ?assertMatch(
        {error, {already_in_use, _}},
        erleans_pm:local_add(GrainRef, self())
    ).


stale_local_entry(Config) ->
    GrainRef = ?config(grainref, Config),

    %% We simulate a previous local reg
    ok = plum_db:put(?PDB_PREFIX, GrainRef, [partisan:self()]),
    dbg:tracer(), dbg:p(all,c), dbg:tpl(erleans_pm, '_', x),
    ?assertMatch(
        undefined,
        erleans_pm:whereis_name(GrainRef),
        "Should not return the pid it is a stale entry and thus "
        "it is not present in the local erleans_pm ets table."
    ).


unreachable_remote_entry(Config) ->
    GrainRef = ?config(grainref, Config),

    %% We simulate a previous remote reg
    [_|PidStr] = partisan:self(),
    PidRef1 = ['foo@127.0.0.1'|PidStr],
    plum_db:put(?PDB_PREFIX, GrainRef, [PidRef1]),

    ?assertMatch(
        undefined,
        erleans_pm:whereis_name(GrainRef, [safe]),
        "Should be unreachable and deemed dead"
    ),

    {ok, 2} = test_grain:call_counter(GrainRef),
    PidRef2 = erleans_pm:whereis_name(GrainRef, [safe]),

    ?assertMatch(
        PidRef2,
        erleans_pm:whereis_name(GrainRef),
        "Safe should be the default"
    ),

    ?assertNotMatch(
        undefined,
        PidRef2,
        "Should be active"
    ),

    ?assertNotMatch(
        [PidRef1, PidRef2],
        plum_db:get(?PDB_PREFIX,GrainRef),
        "We should have 2 pids"
    ).



