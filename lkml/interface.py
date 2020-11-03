"""Serializes a Python dictionary into a LookML string."""

import logging
from lkml.tree import (
    BlockNode,
    ContainerNode,
    DocumentNode,
    ExpressionSyntaxToken,
    LeftBracket,
    LeftCurlyBrace,
    ListNode,
    PairNode,
    QuotedSyntaxToken,
    RightBracket,
    RightCurlyBrace,
    SyntaxNode,
    SyntaxToken,
)
from lkml.visitors import Visitor
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

from lkml.keys import (
    EXPR_BLOCK_KEYS,
    KEYS_WITH_NAME_FIELDS,
    PLURAL_KEYS,
    QUOTED_LITERAL_KEYS,
    pluralize,
    singularize,
)

logger = logging.getLogger(__name__)


def flatten(sequence: list) -> list:
    result = []
    for each in sequence:
        if isinstance(each, list):
            result.extend(each)
        else:
            result.append(each)
    return result


class DictVisitor(Visitor):
    def __init__(self):
        self.depth: int = -1  # Tracks the level of nesting

    def update_tree(self, target: Dict, update: Dict) -> None:
        """Add one dictionary to an existing dictionary, handling certain repeated keys.
        
        This method is primarily responsible for handling repeated keys in LookML like
        `dimension` or `set`, which can exist more than once in LookML but cannot be
        repeated in a Python dictionary.
        
        This method checks the list of valid repeated keys and combines the values of
        that key in `target` and/or `update` into a list and assigns a plural key (e.g.
        `dimensions` instead of `dimension`).
        
        Args:
            target: Existing dictionary of parsed LookML
            update: New dictionary to be added to target
        
        Raises:
            KeyError: If `update` has more than one key
            KeyError: If the key in `update` already exists and would overwrite existing
        
        Examples:
            >>> from pprint import pprint
            >>> parser = Parser((tokens.Token(1),))

            Updating the target with a non-existing, unique key.

            >>> target = {"name": "foo"}
            >>> update = {"sql_table_name": "foo.bar"}
            >>> parser.update_tree(target, update)
            >>> pprint(target)
            {'name': 'foo', 'sql_table_name': 'foo.bar'}

            Updating the target with a non-existing, repeatable key.

            >>> target = {"name": "foo"}
            >>> update = {"dimension": {"sql": "${TABLE}.foo", "name": "foo"}}
            >>> parser.update_tree(target, update)
            >>> pprint(target)
            {'dimensions': [{'name': 'foo', 'sql': '${TABLE}.foo'}], 'name': 'foo'}

            Updating the target with an existing, repeatable key.

            >>> target = {"name": "foo", "dimensions": [{"sql": "${TABLE}.foo", "name": "foo"}]}
            >>> update = {"dimension": {"sql": "${TABLE}.bar", "name": "bar"}}
            >>> parser.update_tree(target, update)
            >>> pprint(target)
            {'dimensions': [{'name': 'foo', 'sql': '${TABLE}.foo'},
                            {'name': 'bar', 'sql': '${TABLE}.bar'}],
             'name': 'foo'}

        """
        keys = tuple(update.keys())
        if len(keys) > 1:
            raise KeyError("Dictionary to update with cannot have multiple keys.")
        key = keys[0]

        if key in PLURAL_KEYS:
            plural_key = pluralize(key)
            if plural_key in target.keys():
                target[plural_key].append(update[key])
            else:
                target[plural_key] = [update[key]]
        elif key in target.keys():
            if self.depth == 0:
                logger.warning(
                    'Multiple declarations of top-level key "%s" found. '
                    "Using the last-declared value.",
                    key,
                )
                target[key] = update[key]
            else:
                raise KeyError(
                    f'Key "{key}" already exists in tree '
                    "and would overwrite the existing value."
                )
        else:
            target[key] = update[key]

    def visit(self, document: DocumentNode) -> Dict[str, Any]:
        return self.visit_container(document.container)

    def visit_container(self, node: ContainerNode) -> Dict[str, Any]:
        container = {}
        if len(node.items) > 0:
            self.depth += 1
            for item in node.items:
                self.update_tree(container, item.accept(self))
            self.depth -= 1
        return container

    def visit_block(self, node: BlockNode) -> Dict[str, Dict]:
        container_dict = node.container.accept(self)
        if node.name is not None:
            container_dict["name"] = node.name.accept(self)
        return {node.type.accept(self): container_dict}

    def visit_list(self, node: ListNode) -> Dict[str, List]:
        return {node.type.accept(self): [item.accept(self) for item in node.items]}

    def visit_pair(self, node: PairNode) -> Dict[str, str]:
        return {node.type.accept(self): node.value.accept(self)}

    def visit_token(self, token: SyntaxToken) -> str:
        return str(token.value)


class DictParser:
    """Parses a Python dictionary into a concrete syntax tree.

    Review the grammar specified for the Parser class to understand how LookML
    is represented. The grammar details the differences between blocks, pairs, keys,
    and values.

    Attributes:
        parent_key: The name of the key at the previous level in a LookML block
        level: The number of indentations appropriate for the current position
        field_counter: The position of the current field when serializing
            iterable objects
        base_indent: Whitespace representing one tab
        indent: An indent of whitespace dynamically sized for the current position
        newline_indent: A newline plus an indent string

    """

    def __init__(self):
        """Initializes the Serializer."""
        self.parent_key: str = None
        self.level: int = 0
        self.base_indent: str = " " * 2
        self.latest_node: Optional[SyntaxNode] = DocumentNode

    def increase_level(self) -> None:
        """Increases the indent level of the current line by one tab."""
        self.latest_node = None
        self.level += 1

    def decrease_level(self) -> None:
        """Decreases the indent level of the current line by one tab."""
        self.level -= 1

    @property
    def indent(self) -> str:
        if self.level > 0:
            return self.base_indent * self.level
        else:
            return ""

    @property
    def newline_indent(self) -> str:
        return "\n" + self.indent

    @property
    def prefix(self) -> str:
        if self.latest_node == DocumentNode:
            return ""
        elif self.latest_node is None:
            return self.newline_indent
        elif self.latest_node == BlockNode:
            return "\n" + self.newline_indent
        else:
            return self.newline_indent

    def is_plural_key(self, key: str) -> bool:
        """Returns True if the key is a repeatable key.

        For example, `dimension` can be repeated, but `sql` cannot be.

        The key `allowed_value` is a special case and changes behavior depending on its
        parent key. If its parent key is `access_grant`, it is a list and cannot be
        repeated. Otherwise, it can be repeated.

        """
        singular_key = singularize(key)
        return singular_key in PLURAL_KEYS and not (
            singular_key == "allowed_value"
            and self.parent_key.rstrip("s") == "access_grant"
        )

    def parse(self, obj: Dict[str, Any]) -> DocumentNode:
        """Returns a LookML string serialized from a dictionary."""
        nodes = [self.parse_any(key, value) for key, value in obj.items()]
        container = ContainerNode(items=tuple(flatten(nodes)))
        return DocumentNode(container)

    def expand_list(
        self, key: str, values: Sequence
    ) -> List[Union[BlockNode, ListNode, PairNode]]:
        """Expands and serializes a list of values for a repeatable key.

        This method is exclusively used for sequences of values with a repeated key like
        `dimensions` or `views`, which need to be serialized sequentially with a newline
        in between.

        Args:
            key: A repeatable LookML field type (e.g. "views" or "dimension_groups")
            values: A sequence of objects to be serialized

        Returns:
            A generator of serialized string chunks

        """
        singular_key = singularize(key)
        nodes = [self.parse_any(singular_key, value) for value in values]
        return flatten(nodes)

    def parse_any(
        self, key: str, value: Union[str, list, tuple, dict]
    ) -> Union[
        List[Union[BlockNode, ListNode, PairNode]], BlockNode, ListNode, PairNode
    ]:
        """Dynamically serializes a Python object based on its type.

        Args:
            key: A LookML field type (e.g. "suggestions" or "hidden")
            value: A string, tuple, or list to serialize

        Raises:
            TypeError: If input value is not of a valid type

        Returns:
            A generator of serialized string chunks

        """
        if isinstance(value, str):
            return self.parse_pair(key, value)
        elif isinstance(value, (list, tuple)):
            if self.is_plural_key(key):
                return self.expand_list(key, value)
            else:
                return self.parse_list(key, value)
        elif isinstance(value, dict):
            if key in KEYS_WITH_NAME_FIELDS or "name" not in value.keys():
                name = None
            else:
                name = value.pop("name")
            return self.parse_block(key, value, name)
        else:
            raise TypeError("Value must be a string, list, tuple, or dict.")

    def parse_block(
        self, key: str, items: Dict[str, Any], name: Optional[str] = None
    ) -> BlockNode:
        """Serializes a dictionary to a LookML block.

        Args:
            key: A LookML field type (e.g. "dimension")
            fields: A dictionary to serialize (e.g. {"sql": "${TABLE}.order_id"})
            name: An optional name of the block (e.g. "order_id")

        Returns:
            A generator of serialized string chunks

        """

        self.parent_key = key
        latest_node_at_this_level = self.latest_node
        self.increase_level()
        nodes = [self.parse_any(key, value) for key, value in items.items()]
        self.decrease_level()
        self.latest_node = latest_node_at_this_level
        container = ContainerNode(items=tuple(flatten(nodes)))

        if self.latest_node and self.latest_node != DocumentNode:
            prefix = "\n" + self.newline_indent
        else:
            prefix = self.prefix

        node = BlockNode(
            type=SyntaxToken(key, prefix=prefix),
            left_brace=LeftCurlyBrace(prefix=" " if name else ""),
            right_brace=RightCurlyBrace(
                prefix=self.newline_indent if container.items else ""
            ),
            name=SyntaxToken(name) if name else None,
            container=container,
        )
        self.latest_node = BlockNode
        return node

    def parse_list(self, key: str, values: Iterable[Union[str, Sequence]]) -> ListNode:
        """Serializes a sequence to a LookML block.

        Args:
            key: A LookML field type (e.g. "fields")
            values: A sequence to serialize (e.g. ["orders.order_id", "orders.item"])

        Returns:
            A generator of serialized string chunks

        """
        # `suggestions` is only quoted when it's a list, so override the default
        force_quote = True if key == "suggestions" else False
        self.parent_key = key

        type_token = SyntaxToken(key, prefix=self.prefix)
        right_bracket = RightBracket()
        items = []
        pair_mode = False

        # Check the first element to see if it's a single value or a pair
        if values and not isinstance(values[0], (str, int)):
            pair_mode = True

        # Choose newline delimiting or space delimiting based on contents
        if len(values) >= 5 or pair_mode:
            trailing_comma = True
            self.increase_level()
            for value in values:
                if pair_mode:
                    # Extract key and value from dictionary with only one key
                    [(key, val)] = value.items()
                    item: PairNode = self.parse_pair(key, val)
                else:
                    item: SyntaxToken = self.parse_token(
                        key, value, force_quote, prefix=self.newline_indent
                    )
                items.append(item)
            self.decrease_level()
            right_bracket.prefix = self.newline_indent
        else:
            trailing_comma = False
            for i, value in enumerate(values):
                if i == 0:
                    token = self.parse_token(key, value, force_quote)
                else:
                    token = self.parse_token(key, value, force_quote, prefix=" ")
                items.append(token)

        node = ListNode(
            type=type_token,
            left_bracket=LeftBracket(),
            items=tuple(items),
            right_bracket=right_bracket,
            trailing_comma=trailing_comma,
        )
        self.latest_node = ListNode
        return node

    def parse_pair(self, key: str, value: str) -> PairNode:
        """Serializes a key and value to a LookML pair.

        Args:
            key: A LookML field type (e.g. "hidden")
            value: The value string (e.g. "yes")

        Returns:
            A generator of serialized string chunks

        """
        force_quote = True if self.parent_key == "filters" else False
        value_syntax_token: SyntaxToken = self.parse_token(key, value, force_quote)
        node = PairNode(
            type=SyntaxToken(key, prefix=self.prefix), value=value_syntax_token
        )
        self.latest_node = PairNode
        return node

    @staticmethod
    def parse_token(
        key: str,
        value: str,
        force_quote: bool = False,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
    ) -> SyntaxToken:
        """Parses a value into a token, quoting it if required by the key or forced.

        Args:
            key: A LookML field type (e.g. "hidden")
            value: The value string (e.g. "yes")
            force_quote: True if value should always be quoted

        Returns:
            A generator of serialized string chunks

        """
        if force_quote or key in QUOTED_LITERAL_KEYS:
            return QuotedSyntaxToken(value, prefix, suffix)
        elif key in EXPR_BLOCK_KEYS:
            return ExpressionSyntaxToken(value.strip() + " ", prefix, suffix)
        else:
            return SyntaxToken(value, prefix, suffix)
