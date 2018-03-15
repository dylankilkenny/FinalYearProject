import React, { Component } from "react";
import ReactDOM from "react-dom";
import Table from "../presentational/Table";

class CurrencyContainer extends React.Component {
    constructor(props) {
        super(props);
        this.state = {}
        this.storeCurrency = this.storeCurrency.bind(this);
    }

    componentDidMount() {
        const apiUrl = "http://localhost:3000/OneCurrency"
        console.log(this.props.match.params.currency)
        const payload = { id: this.props.match.params.currency }
        fetch(apiUrl, {
            method: "POST",
            body: JSON.stringify(payload),
            headers: { 'Content-Type': 'application/json' },
        })
            .then(response => response.json())
            .then(data => {
                console.log(data)
                this.storeCurrency(data)
            })
            .catch(error => console.log(error))
    }

    componentDidCatch(error, info) {
        console.log(error)
        console.log(info)
    }

    storeCurrency = data => {
        this.setState(data)
    }

    render() {
        return (
            <section>
                <div>
                    <h1> {this.state.name} </h1>
                    <p>Symbol: {this.state.symbol}</p>
                    <p>Rank: {this.state.rank}</p>
                    <p>Price: {this.state.price_usd}</p>
                    <p>Market Cap: {this.state.market_cap_usd}</p>
                    <p>Social Sentiment: {this.state.social_sentiment}</p>
                    <p>Social Volume: {this.state.social_volume}</p>
                </div>
            </section>
        )
    }
}
export default CurrencyContainer