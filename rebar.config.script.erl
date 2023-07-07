%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 ft=erlang et
%%

%% We will add the location of the OTP patched modules based on the
%% OTP version
Vsn = erlang:system_info(otp_release),
%% CONFIG is a special variable injected by rebar3.
Eqwalizer =
    case os:getenv("EQWALIZER", false) of
        Var when Var == "1" orelse Var == "true" ->
            true;
        _ ->
            false
    end,


%% Dynamically adds the Partisan OTP sources to the src_dirs configuration key,
%% based on the OTP version.
SetSrcDirs = fun
    (V, Acc) when V >= "24" ->
        Default = ["src"],
        SrcDirs =
            case lists:keyfind(src_dirs, 1, Acc) of
                {src_dirs, Val} ->
                    Val ++ ["priv/otp/24"];
                false ->
                    Default ++ ["priv/otp/24"]
            end,
        lists:keystore(src_dirs, 1, Acc, {src_dirs, SrcDirs});

    (V, _) ->
        exit("OTP version " ++ V ++ " not supported by Partisan.")
end,

%% Adds Eqwalizer support only if Vsn >= "25".
%% We do this as the Eqwalizer executable doesn't allow us to use a separate
%% rebar3 profile.
MaybeEqwalizerSupport = fun
    (V, Acc0) when V >= "25" ->
        {profiles, Profiles0} = lists:keyfind(profiles, 1, Acc0),
        {test, Test0} = lists:keyfind(test, 1, Profiles0),
        {deps, Deps0} = lists:keyfind(deps, 1, Test0),
        Deps = Deps0 ++ [
            {eqwalizer_support,
                {
                  git_subdir,
                  "https://github.com/whatsapp/eqwalizer.git",
                  {tag, "v0.17.16"},
                  "eqwalizer_support"
                }
            }
        ],
        Test = lists:keystore(deps, 1, Test0, {deps, Deps}),
        Profiles = lists:keystore(test, 1, Profiles0, {test, Test}),
        Acc1 = lists:keystore(profiles, 1, Acc0, {profiles, Profiles}),

        {project_plugins, Plugs0} = lists:keyfind(project_plugins, 1, Acc1),
        Plugs = Plugs0 ++ [
            {eqwalizer_rebar3,
              {
                git_subdir,
                "https://github.com/whatsapp/eqwalizer.git",
                {branch, "main"},
                "eqwalizer_rebar3"
              }
            }
        ],
        lists:keystore(deps, 1, Acc1, {project_plugins, Plugs});

    (_, Acc) ->
        Acc
end,

case Eqwalizer of
    true ->
        %% We do not want to add OTP sources as we are not modifying those
        %% sources to fix eqwalizer warnings, so that the diff between the
        %% original and ours is just due to partisan requirements.
        MaybeEqwalizerSupport(Vsn, SetSrcDirs(Vsn, CONFIG));
    false ->
        SetSrcDirs(Vsn, CONFIG)
end.



