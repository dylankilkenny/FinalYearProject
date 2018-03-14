import React, { Component } from "react";
import ReactDOM from "react-dom";
import Table from "../presentational/Table";

class TableContainer extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
          coins: []
        }
        this.storeCoins = this.storeCoins.bind(this);
    }

    componentDidMount() {
        const apiUrl = "https://api.coinmarketcap.com/v1/ticker/"
        fetch(apiUrl)
            .then(response => response.json())
            .then(data => this.storeCoins(data))
            .catch(error => console.log(error))
    }

    componentDidCatch(error, info) {
        console.log(error)
        console.log(info)
    }

    storeCoins = data => {
        const coins = data.map( result => {
          const  { id, name, symbol, rank, price_usd, market_cap_usd } = result;
          return { id, name, symbol, rank, price_usd, market_cap_usd };
        });
    
        this.setState({ coins })
    }
    
    render() {
        return (
          <section>
            <div>
                <Table coins= {this.state.coins}/>
            </div>
          </section>
        )
    }
  }
  export default TableContainer