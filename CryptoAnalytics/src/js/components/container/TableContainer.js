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
        const apiUrl = "http://localhost:3000/AllCurrencies"
        fetch(apiUrl)
            .then(response => response.json())
            .then(data => {
                console.log(data)
                this.storeCoins(data)
            })
            .catch(error => console.log(error))
    }

    componentDidCatch(error, info) {
        console.log(error)
        console.log(info)
    }

    storeCoins = data => {
        const coins = data.map( result => {
          const  { id, name, symbol, rank, price_usd, market_cap_usd, social_volume, social_sentiment } = result;
          return { id, name, symbol, rank, price_usd, market_cap_usd, social_volume, social_sentiment };
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