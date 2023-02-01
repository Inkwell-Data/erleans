-module(erleans_monitor).

-behavior(partisan_gen_server).

-export([start_link/0,
         monitor/2,
         demonitor/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(TAB, ?MODULE).

-record(state, {}).

start_link() ->
    case ets:info(?TAB, name) of
        undefined ->
            ets:new(?TAB, [bag, public, named_table,
                           {write_concurrency, true},
                           {read_concurrency, true}]);
        _ ->
            ok
    end,
    partisan_gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec monitor(GrainRef :: erleans:grain_ref(), Pid :: pid())
        -> ok.
monitor(GrainRef, Pid) ->
    _ = ets:insert(?TAB, {Pid, GrainRef}),
    partisan_gen_server:cast(?MODULE, {monitor_me, Pid}).

-spec demonitor(GrainRef :: erleans:grain_ref(), Pid :: pid())
        -> true.
demonitor(GrainRef, Pid) ->
    ets:delete_object(?TAB, {Pid, GrainRef}).

init([]) ->
    {ok, #state{}}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast({monitor_me, Pid}, State) ->
    _ = erlang:monitor(process, Pid),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _}, S) ->
    _ = process_down(Pid),
    {noreply, S};
handle_info(_, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% @private
process_down(Pid) when is_pid(Pid) ->
    [begin
         unregister_name(Name, Pid),
         ets:delete(?TAB, Pid)
     end || {_, Name} <- ets:lookup(?TAB, Pid)].

%% @private
unregister_name(GrainRef, Pid) ->
    erleans_pm:plum_db_remove(GrainRef, Pid).
