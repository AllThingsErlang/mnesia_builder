-module(move_beam).
-export([start/0, loop/0]).

start() ->
    register(move_beam, spawn(fun loop/0)).

loop() ->
    % Perform the file moving operation
    case move_beam_files() of
        ok -> 
            timer:sleep(2000);  % Sleep for 2 seconds before continuing
        {error, Reason} -> 
            io:format("~n~p~n", [{error, Reason}]),
            exit({error, Reason})  % Abort on error
    end,

    % Check for messages, allowing the process to terminate gracefully
    receive
        quit ->
            ok;  % Terminate the process
        _ ->
            loop()  % Continue looping
    after 0 ->
        loop()  % Timeout immediately and continue looping
    end.

move_beam_files() ->
    SrcDir = ".",
    EbinDir = "../ebin",
    case file:list_dir(SrcDir) of
        {ok, Files} ->
            BeamFiles = [F || F <- Files, filename:extension(F) =:= ".beam"],
            lists:foldl(fun(File, Acc) ->
                case Acc of
                    ok ->
                        SrcPath = filename:join(SrcDir, File),
                        DestPath = filename:join(EbinDir, File),
                        io:format("~s to ~s~n", [SrcPath, DestPath]),
                        file:rename(SrcPath, DestPath);
                    Error -> 
                        Error  % Stop processing on the first error
                end
            end, ok, BeamFiles);
        {error, Reason} ->
            {error, Reason}  % Abort if the source directory cannot be listed
    end.
