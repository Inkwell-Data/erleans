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
            unreachable_remote_entry,
            reachable_stale_remote_entry
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

    %% We simulate a previous local registration. This case can happen
    %% when there was a registration on a previous instantiation of this node
    %% that remained in the global store (another node's replica)
    %% and re-emerges here via active anti-entropy (plum_db).
    ok = plum_db:put(?PDB_PREFIX, GrainRef, [partisan:self()]),

    ?assertMatch(
        undefined,
        erleans_pm:whereis_name(GrainRef),
        "Should not return the pid it is a stale entry and thus "
        "it is not present in the local erleans_pm ets table."
    ),
    ok = plum_db:delete(?PDB_PREFIX, GrainRef).


unreachable_remote_entry(Config) ->
    GrainRef = ?config(grainref, Config),

    %% We simulate a previous remote registration. It could be alive or dead
    %% but as we are not connected to that node we should consider it dead.
    [_|PidStr] = partisan:self(),
    UnreachableNode = 'foo@127.0.0.1',
    PidRef1 = [UnreachableNode|PidStr],
    plum_db:put(?PDB_PREFIX, GrainRef, [PidRef1]),

    ?assertMatch(
        undefined,
        erleans_pm:whereis_name(GrainRef, [safe]),
        "Should be unreachable and considered dead becuase node is not connected"
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
    ),

    ok = plum_db:delete(?PDB_PREFIX, GrainRef).


reachable_stale_remote_entry(Config) ->
    GrainRef = ?config(grainref, Config),

    %% We simulate a previous registration. This case can happen
    %% when there was a registration on a previous instantiation of a node
    %% that remained in the global store (another node's replica)
    %% and re-emerges here via active anti-entropy (plum_db).
    [_|PidStr] = partisan:self(),
    Node = 'foo@127.0.0.1',
    PidRef1 = [Node|PidStr],
    plum_db:put(?PDB_PREFIX, GrainRef, [PidRef1]),


    meck:new(erleans_pm, [passthrough]),
    meck:expect(erleans_pm, grain_ref,
        fun
            (Pid) when is_pid(Pid) ->
                meck:passthrough(Pid);

            (PidRef) ->
                case partisan:node(PidRef) == partisan:node() of
                    true ->
                        meck:passthrough(partisan_remote_ref:to_term(PidRef));
                    false ->
                        %% We simulate the remote grain is reachable and returns
                        %% a diff Ref
                        GrainRef#{id => foo}
                end
        end
    ),

    ?assertMatch(
        PidRef1,
        erleans_pm:whereis_name(GrainRef, [unsafe]),
        "Should return Pid even though is the wrong one as it is associated "
        "with a different Grain, but based on our copy of the global store it "
        "is still associated with it"
    ),

    ?assertMatch(
        undefined,
        erleans_pm:whereis_name(GrainRef, [safe]),
        "Should be unreachable and considered dead becuase node is not connected"
    ),

    ok.




