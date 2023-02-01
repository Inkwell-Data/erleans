%%%--------------------------------------------------------------------
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
%%%
%% @doc
%% @end
%%%-------------------------------------------------------------------

-module(erleans_app).

-behaviour(application).

-export([start/2,
         stop/1]).

-include_lib("kernel/include/logger.hrl").

start(_StartType, _StartArgs) ->
    Config = application:get_all_env(erleans),
    %% plum_db will start partisan
    _ = application:ensure_all_started(plum_db, permanent),
    erleans_sup:start_link(Config).

stop(_State) ->
    erleans_cluster:leave(),
    ok.

%% Internal functions
