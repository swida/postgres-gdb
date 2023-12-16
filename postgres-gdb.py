import os.path
import re
import subprocess
import argparse
from autocvar import autocvar, AutoNumCVar


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

class TreeWalker(object):
    """A base class for tree traverse"""

    SHOW_FUNC_PREFIX = 'show_'
    WALK_FUNC_PREFIX = 'walk_'

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
        expr_typed = expr.dynamic_type
        expr_casted = expr.cast(expr_typed)
        self.current_level = level
        level_graph = '  '.join(self.level_graph[:level])
        for i, c in enumerate(self.level_graph):
            if c == '`':
                self.level_graph[i] = ' '
        cname = self.autoncvar.set_var(expr_casted)
        left_margin = "{}{}".format('' if level == 0 else '--', cname)
        element_show_info = ''
        show_func = self.get_action_func(expr_typed, self.SHOW_FUNC_PREFIX)
        if show_func is not None:
            element_show_info = show_func(expr_casted)
        if element_show_info is not None:
            print("{}{} ({}) {} {}".format(
                  level_graph, left_margin, expr_typed, expr, element_show_info))
        walk_func = self.get_action_func(expr_typed, self.WALK_FUNC_PREFIX)
        if walk_func is None:
            return
        children = walk_func(expr_casted)
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
            if typ.code == gdb.TYPE_CODE_PTR:
                typ = typ.target()
            return typ.name if hasattr(typ, 'name') and typ.name is not None else str(typ)
        func_name = action_prefix + type_name(element_type)
        if hasattr(self, func_name) and callable(getattr(self, func_name)):
            return getattr(self, func_name)

        for field in element_type.fields():
            if not field.is_base_class:
                continue
            typ = field.type
            func_name = action_prefix + type_name(typ)

            if hasattr(self, func_name):
                return getattr(self, func_name)

            return self.get_action_func(typ, action_prefix)

        # Fall through to common action function
        if hasattr(self, action_prefix) and callable(getattr(self, action_prefix)):
            return getattr(self, action_prefix)

        return None

def cast_Node(val):
    if val.type.target().code == gdb.TYPE_CODE_VOID:
        val = val.cast(gdb.lookup_type("Node").pointer())
    node_type = str(val['type'])
    typ = gdb.lookup_type(node_type[2:])
    return val.cast(typ.pointer())

class ListCell:
    ptr = 1
    Int = 451
    Oid = 452
    Xid = 453
    type_values = {ptr : 'ptr_value', Int : 'int_value',
                   Oid : 'oid_value', Xid : 'xid_value'}
    def __init__(self, typ, val, val_type):
        self.value_type = typ
        self.value = val[self.type_values[typ]]
        if typ != self.ptr:
            return
        self.value = cast_Node(self.value) if \
            val_type.name == 'Node' else self.value.cast(val_type.pointer())
    def to_string(self, cvar):
        if self.value_type != self.ptr:
            return self.value
        type_name = self.value.dereference().type.name
        return f'{cvar} ({type_name} *) '  + str(self.value)

class List:
    def  __init__(self, val, ptr_type_name = None):
        self._type = int(val['type'])
        self.type_name = ListCell.type_values[self._type]
        self.length = int(val["length"])
        self._elements = val["elements"]
        self._index = 0
        self._ptr_type = gdb.lookup_type(ptr_type_name) if ptr_type_name else None

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= self.length:
            raise StopIteration
        cell = ListCell(self._type, self._elements[self._index], self._ptr_type)
        self._index += 1
        return cell

class ExprTraverser(gdb.Command, TreeWalker):
    def __init__ (self):
        super(self.__class__, self).__init__ ("pg expr", gdb.COMMAND_DATA)

    def walk_List(self, val):
        children = []
        for ele in List(val, "Node"):
            children.append(ele.value)
        return children
    def _walk_to_args(self, val):
        return self.walk_List(val['args']) if gdb.types.has_field(val.type.target(), 'args') else []
    def walk_(self, val):
        return self._walk_to_args(val)
    def show_(self, val):
        # Display properties for each struct, can define as single string if
        # only 1 property needs to display. if a property name includes ':',
        # a property value can be appended after ':'. It will be hided if
        # the property value does not equal to the value.
        display_fields = {
            'Const' : ('constisnull:true', 'consttype', 'constvalue'),
            'Var' : ('varno', 'varattno', 'vartype'),
            'BoolExpr' : 'boolop',
            'OpExpr' : 'opno',
            'ScalarArrayOpExpr' : 'opno',
            'FuncExpr' : ('funcid', 'funcresulttype')
            }
        typname = val.dereference().type.name
        def prop_str(prop):
            if ':' in prop:
                prop, disp_val = prop.split(':')
                if disp_val != str(val[prop]):
                    return ''
            name = prop[len(typname):] if prop.startswith(typname.lower()) else prop
            return f'{name} = {val[prop]}'
        if typname not in display_fields:
            return ''
        prop = display_fields[typname]
        return prop_str(prop) if isinstance(prop, str) else \
            ', '.join([prop_str(p) for p in prop if prop_str(p)])

    def invoke(self, arg, from_tty):
        if not arg:
            print("usage: pg expr [expr]")
            return
        expr = gdb.parse_and_eval(arg)
        self.walk(expr)
ExprTraverser()

class PlanTraverser(gdb.Command, TreeWalker):
    def __init__ (self):
        super(self.__class__, self).__init__ ("pg plan", gdb.COMMAND_DATA)

    def walk_(self, val):
        children = []
        typ = gdb.lookup_type('Plan')
        plan = val.cast(typ.pointer())
        for field in ('lefttree', 'righttree'):
            if plan[field]:
                child = plan[field]
                children.append(cast_Node(child))
        return children

    def show_scan(self, val):
        relid = val['scan']['scanrelid']
        return f'scanrelid={relid}'

    show_SeqScan = show_scan

    def invoke(self, arg, from_tty):
        if not arg:
            print("usage: pg plan [plan]")
            return
        plan = gdb.parse_and_eval(arg)
        self.walk(cast_Node(plan))
PlanTraverser()

class NodeCastPrinter(gdb.Command):
    def __init__ (self):
        super(self.__class__, self).__init__ ("pg node", gdb.COMMAND_DATA)
    def invoke(self, arg, from_tty):
        if not arg:
            print("usage: pg plan [plan]")
            return
        node = gdb.parse_and_eval(arg)
        val = cast_Node(node)
        cname = AutoNumCVar().set_var(val)
        print(f'{cname} ({val.type})', val)
NodeCastPrinter()

class ListPrinter:
    """Pretty-printer for List."""
    def __init__(self, val):
        self.val = List(val, "Node")
        self.autoncvar = AutoNumCVar()

    def display_hint(self):
        return "array"

    def to_string(self):
        return f"List with {self.val.length} {self.val.type_name} elements"

    def children(self):
        for i, elt in enumerate(self.val):
            cvar = autocvar.set_var(elt.value)
            yield (str(i), elt.to_string(cvar))

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
