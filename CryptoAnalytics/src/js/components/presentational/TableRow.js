import React from "react";
import PropTypes from "prop-types";
import { Icon, Label, Menu, Table } from "semantic-ui-react"

const TableRow = (props) => (
    <Table.Row>
        <Table.Cell>{props.rank}</Table.Cell>
        <Table.Cell>{props.name}</Table.Cell>
        <Table.Cell>${props.price_usd}</Table.Cell>
        <Table.Cell>{props.symbol}</Table.Cell>
        <Table.Cell>${props.market_cap_usd}</Table.Cell>
    </Table.Row>
)

export default TableRow