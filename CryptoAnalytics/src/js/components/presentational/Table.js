import React from "react";
import PropTypes from "prop-types";
import { Icon, Label, Menu, Table } from "semantic-ui-react"
import TableRow from "./TableRow"

const TableCoin = (props) => (
  <Table celled>
    <Table.Header>
      <Table.Row>
        <Table.HeaderCell>Rank</Table.HeaderCell>
        <Table.HeaderCell>Name</Table.HeaderCell>
        <Table.HeaderCell>Price</Table.HeaderCell>
        <Table.HeaderCell>Symbol</Table.HeaderCell>
        <Table.HeaderCell>Market Cap</Table.HeaderCell>
      </Table.Row>
    </Table.Header>

    <Table.Body> 
        {
        props.coins.map( (coin) => (
            <TableRow 
            rank={coin.rank} 
            name={coin.name} 
            price_usd={coin.price_usd} 
            symbol={coin.symbol} 
            market_cap_usd={coin.market_cap_usd} 
            />
        ))
        }
    </Table.Body>
  </Table>
);

// export default TableExamplePagination
// const Input = ({ label, text, type, id, value, handleChange }) => (
//   <div className="form-group">
//     <label htmlFor={label}>{text}</label>
//     <input
//       type={type}
//       className="form-control"
//       id={id}
//       value={value}
//       onChange={handleChange}
//       required
//     />
//   </div>
// );
// Input.propTypes = {
//   label: PropTypes.string.isRequired,
//   text: PropTypes.string.isRequired,
//   type: PropTypes.string.isRequired,
//   id: PropTypes.string.isRequired,
//   value: PropTypes.string.isRequired,
//   handleChange: PropTypes.func.isRequired
// };
export default TableCoin