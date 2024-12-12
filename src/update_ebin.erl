-module(update_ebin).
-export([start/0, loop/0]).

start() ->
    register(?MODULE, spawn(fun loop/0)).

loop() ->
    timer:sleep(2000),

    SrcDir = "../src",
    EbinDir = "../ebin",

    % Perform the file moving operation
    Result1 = handle_files(move, SrcDir, EbinDir, ".beam"),
    Result2 = handle_files(copy, SrcDir, EbinDir, ".app"),

    % Check for messages, allowing the process to terminate gracefully
    receive
        quit ->
            ok;  % Terminate the process

        result ->
            io:format("~nbeam: ~p~n", [Result1]),
            io:format("app: ~p~n~n", [Result2]),
            loop();

        _ ->
            loop()  % Continue looping

    after 0 ->
        loop()  % Timeout immediately and continue looping
    end.



handle_files(Operation, FromDir, ToDir, Extension) ->

    case file:list_dir(FromDir) of
        {ok, Files} ->
            BeamFiles = [F || F <- Files, filename:extension(F) =:= Extension],
            lists:foldl(fun(File, Acc) ->
                case Acc of
                    ok ->
                        SrcPath = filename:join(FromDir, File),
                        DestPath = filename:join(ToDir, File),

                        case Operation of 
                            move -> file:rename(SrcPath, DestPath);
                            copy -> file:copy(SrcPath, DestPath) 
                        end;

                    Error -> 
                        Error  % Stop processing on the first error
                end
            end, ok, BeamFiles);
        {error, Reason} ->
            {error, Reason}  % Abort if the source directory cannot be listed
    end.


