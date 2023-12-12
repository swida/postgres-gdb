import os.path
import re
import subprocess
import argparse
from autocvar import autocvar, AutoNumCVar

class TreeWalker(object):
    """A base class for tree traverse"""

    def __init__(self):
        self.level_graph = []
        self.autoncvar = None
        self.current_level = 0

    def reset(self):
        self.level_graph = []
        self.autoncvar = AutoNumCVar()

    def walk(self, expr):
        self.reset()
        self.do_walk(expr, 0)

    def do_walk(self, expr, level):
        expr_type = expr.dynamic_type
        expr_nodetype = None
        try:
            expr_nodetype = expr.type.template_argument(0)
            if expr_nodetype.code != gdb.TYPE_CODE_PTR:
                expr_nodetype = expr.type.template_argument(0).pointer()
        except (gdb.error, RuntimeError):
            expr_nodetype = None
            pass

        # There is no inheritance in C, we must do cast by calling
        # cast_[TYPE] API.
        cast_func = self.get_cast_func(expr_type, expr_nodetype)
        if cast_func is not None:
            explicit_cast_type = cast_func(expr)
            expr_casted = expr.cast(explicit_cast_type)
        else:
            explicit_cast_type = None
            expr_casted = expr.cast(expr_type)

        self.current_level = level
        level_graph = '  '.join(self.level_graph[:level])
        for i, c in enumerate(self.level_graph):
            if c == '`':
                self.level_graph[i] = ' '
        cname = self.autoncvar.set_var(expr_casted)
        left_margin = "{}{}".format('' if level == 0 else '--', cname)

        element_show_info = ''
        show_func = None if explicit_cast_type is None else self.get_show_func(explicit_cast_type, expr_nodetype)
        if show_func:
            element_show_info = show_func(expr_casted)
        else:
            show_func = self.get_show_func(expr_type, expr_nodetype)
            if show_func is not None:
                element_show_info = show_func(expr if explicit_cast_type else expr_casted)
        if element_show_info is not None:
            expr_disp_type = explicit_cast_type if explicit_cast_type else expr_type
            print(f"{level_graph}{left_margin} ({expr_disp_type}) {expr} {element_show_info}")

        walk_func = None if explicit_cast_type is None else self.get_walk_func(explicit_cast_type, expr_nodetype)
        if walk_func:
            children = walk_func(expr_casted)
        else:
            walk_func = self.get_walk_func(expr_type, expr_nodetype)
            if walk_func is None:
                return
            children = walk_func(expr if explicit_cast_type else expr_casted)
        if not children:
            return
        if len(self.level_graph) < level + 1:
            self.level_graph.append('|')
        else:
            self.level_graph[level] = '|'
        for i, child in enumerate(children):
            if i == len(children) - 1:
                self.level_graph[level] = '`'
            self.do_walk(child, level + 1)

    def get_action_func(self, element_type, action_prefix):
        def type_name(typ):
            return typ.name if hasattr(typ, 'name') and typ.name is not None else str(typ)
        func_name = action_prefix + type_name(element_type)
        if hasattr(self, func_name):
            return getattr(self, func_name)

        for field in element_type.fields():
            if not field.is_base_class:
                continue
            typ = field.type
            func_name = action_prefix + type_name(typ)

            if hasattr(self, func_name):
                return getattr(self, func_name)

            return self.get_action_func(typ, action_prefix)
        return None

    def get_walk_func(self, element_type, element_type_templ):
        return self.get_action_func(element_type_templ.target(), 'walk_templ_') \
            if element_type_templ is not None else self.get_action_func(element_type.target(), 'walk_')

    def get_show_func(self, element_type, element_type_templ):
        return self.get_action_func(element_type_templ.target(), 'show_templ_') \
            if element_type_templ is not None else self.get_action_func(element_type.target(), 'show_')

    def get_cast_func(self, element_type, element_type_templ):
        return self.get_action_func(element_type_templ.target(), 'cast_templ_') \
            if element_type_templ is not None else self.get_action_func(element_type.target(), 'cast_')

gdb.Command('pg', gdb.COMMAND_DATA, prefix=True)

class BackendAttach(gdb.Command):
    """attach user postgres backend"""

    def __init__ (self):
        super (self.__class__, self).__init__ ("pg attach", gdb.COMMAND_RUNNING)
        self.backends = ()
        self.pid = None

    def attach(self):
        gdb.execute(f"attach {self.pid}")

    def grab_backends(self, datadir):
        pidfile = os.path.expanduser(os.path.join(datadir.string(), 'postmaster.pid'))
        master_pid = 0
        with open(pidfile) as f:
            master_pid = f.readline().strip()
        if not master_pid:
            return
        pscmd = 'ps -o pid=,cmd= --ppid=%s' % master_pid
        output = subprocess.getoutput(pscmd)
        def split_cmdline(line):
            match = re.match(r'\s*(\d+)\s+', line)
            return (match.group(1), line[match.end():])
        processes = [split_cmdline(line) for line in output.split('\n')]
        ignore_titles = ('postgres: checkpointer', 'postgres: background writer', 'postgres: walwriter',
                         'postgres: autovacuum launcher', 'postgres: logical replication launcher')
        backends = [(int(p[0]), p[1]) for p in filter(lambda p : p[1].strip() not in ignore_titles, processes)]
        if not backends:
            gdb.write("No postgres backend found\n")
            return
        self.backends = backends

    def print_backends(self):
        for i, (pid, title) in enumerate(self.backends):
            print(f"{i + 1}. {pid} {title}")

    def invoke (self, arg, from_tty):
        argv = gdb.string_to_argv(arg)
        parser = argparse.ArgumentParser()
        parser.add_argument('-f', '--force', action='store_true')
        parser.add_argument('-i', '--interactive', action='store_true')
        parser.add_argument('-l', '--list', action='store_true')
        args = parser.parse_args(argv)
        cur = gdb.selected_inferior()
        if not args.force and cur.pid != 0:
            gdb.write(f"Process {cur.pid} has already attached.\n")
            return
        self.grab_backends(gdb.convenience_variable("datadir"))
        if not self.backends:
            return
        self.pid = self.backends[0][0]

        if args.list:
            self.print_backends()
            return

        multiple = len(self.backends) > 1
        if multiple:
            gdb.write("There are more than 1 backends:\n")
            self.print_backends()
            if args.interactive:
                index = int(input("Select backend:"))
                if index <= 0 or index > len(self.backends):
                    gdb.write("Invalid process input")
                    return
                self.pid = self.backends[index - 1][0]
        if cur.pid != 0:
            gdb.execute("detach")
        self.attach()
BackendAttach()

class NodeTraverser(TreeWalker):
    def walk_Node(self, val):
        sub_nodes = []
        if val['lefttree']:
            sub_nodes.append(val['lefttree'])
        if val['righttree']:
            sub_nodes.append(val['righttree'])
        return sub_nodes

    walk_Plan = walk_Node

    def cast_Plan(self, val):
        node_type = str(val['type'])
        typ = gdb.lookup_type(node_type[2:])
        return typ.pointer()

    def show_scan(self, val):
        relid = val['scan']['scanrelid']
        return f'scanrelid={relid}'

    show_SeqScan = show_scan

class PlanTraverser(gdb.Command, NodeTraverser):
    def __init__ (self):
        super(self.__class__, self).__init__ ("pg plan", gdb.COMMAND_DATA)

    def invoke(self, arg, from_tty):
        if not arg:
            print("usage: pg plan [plan]")
            return
        plan = gdb.parse_and_eval(arg)
        self.walk(plan)
PlanTraverser()

class NodeCast:
    def __init__(self, val):
        self._val = val
        node_type = gdb.lookup_type('Node')
        self.explicit_type_name = str(self._val.cast(node_type.pointer())['type'])[2:]
        explicit_type = gdb.lookup_type(self.explicit_type_name)
        self.explicit_val = self._val.cast(explicit_type.pointer())

    def to_string(self, autoncvar):
        cvar = autoncvar.set_var(self.explicit_val)
        return f'{cvar} ({self.explicit_type_name} *)' + str(self.explicit_val)

class ListCell:
    """Print a ListCell object."""
    ptr = 1
    Int = 451
    Oid = 452
    Xid = 453
    def __init__(self, typ, val):
        self._typ = typ
        self._val = val

    def to_string(self, autoncvar):
        typvalues = {ListCell.ptr : 'ptr_value', ListCell.Int : 'int_value',
                     ListCell.Oid : 'oid_value', ListCell.Xid : 'xid_value'}
        val = self._val[typvalues[self._typ]]
        return NodeCast(val).to_string(autoncvar) if self._typ == ListCell.ptr else val

class ListPrinter:
    """Pretty-printer for List."""
    def __init__(self, val):
        self._val = val
        self._typ = int(self._val['type'])
        self.autoncvar = AutoNumCVar()

    def display_hint(self):
        return "array"

    def to_string(self):
        typnames = {ListCell.ptr : 'ptr', ListCell.Int : 'Int',
                    ListCell.Oid : 'Oid', ListCell.Xid : 'Xid'}
        length = int(self._val['length'])
        return f"List with {length} {typnames[self._typ]} elements"

    def children(self):
        length = int(self._val["length"])
        elements = self._val["elements"]

        child_i = 0
        for elt in range(length):
            cell = elements[elt]
            listcell = ListCell(self._typ, cell)
            yield (str(child_i), listcell.to_string(self.autoncvar))
            child_i += 1
def register_pretty_printer(objfile):
    """A routine to register a pretty-printer against the given OBJFILE."""
    objfile.pretty_printers.append(type_lookup_function)

def type_lookup_function(val):
    """A routine that returns the correct pretty printer for VAL
    if appropriate.  Returns None otherwise.
    """
    tag = val.type.tag
    name = val.type.name
    if name == "List":
        return ListPrinter(val)
    return None

if __name__ == "__main__":
    if gdb.current_objfile() is not None:
        # This is the case where this script is being "auto-loaded"
        # for a given objfile.  Register the pretty-printer for that
        # objfile.
        register_pretty_printer(gdb.current_objfile())
    else:
        for objfile in gdb.objfiles():
            if os.path.basename(objfile.filename) == "postgres":
                objfile.pretty_printers.append(type_lookup_function)
