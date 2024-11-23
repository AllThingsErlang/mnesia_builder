-module(prototype).

%
% Attributes (subset of mnesia specs)
%   {name, Name} - table name, record name  (same name)
%   {disc_copies, Nodelist}
%   {disc_only_copies, Nodelist}
%   {ram_copies, Nodelist}
%   {type, Type} - Type: set, ordered_set or bag
%
install(Attributes) when is_list(Nodes) and is_atom(SchemaName) ->

    mnesia:create_schema(Nodes),
    mnesia:start(),

    % Extract the specified attributes. If not found, assume defaults.
    case lists:keyfind(type, 1, Attributes) of
        {type, Type} -> ok;
        false -> Type = set
    end,


    mnesia:create_table(SchemaName, [
        {disc_copies, [node()]},
        {attributes, record_info(fields, SchemaName)},
        {type, set}
    ]),

    mnesia:stop(),
    ok.
