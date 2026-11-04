"""Catálogo recursivo derivado do argparse real, inclusive alternativas e subcomandos."""
import argparse


def describe(parser):
    arguments, children = [], []
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            helps = {item.dest: item.help for item in action._choices_actions}
            children.extend(dict(command=name, help=helps.get(name, ""), **describe(child))
                            for name, child in action.choices.items())
            continue
        if action.dest == "help":
            continue
        default = None if action.default is argparse.SUPPRESS else action.default
        arguments.append(dict(flags=action.option_strings or [action.dest], dest=action.dest,
            required=bool(action.required) if action.option_strings else action.nargs not in ("?", "*"),
            default=default, choices=list(action.choices) if action.choices is not None else None,
            type=getattr(action.type, "__name__", "string"), nargs=action.nargs,
            action=type(action).__name__, help=action.help or ""))
    groups = [dict(required=group.required, arguments=[item.dest for item in group._group_actions])
              for group in parser._mutually_exclusive_groups]
    return dict(arguments=arguments, subcommands=children, mutually_exclusive=groups)


def command_tree():
    from ..cli import build_parser
    return describe(build_parser())
