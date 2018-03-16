import React, { Component } from "react";
import ReactDOM from "react-dom";
import { Switch, Route, BrowserRouter as Router } from 'react-router-dom'
import TableContainer from "./TableContainer.js";
import CurrencyContainer from "./CurrencyContainer.js"



const Main = () => (
    <Router>
        <div>
            <Switch>
                <Route exact path='/' component={TableContainer} />
                <Route path='/currency/:currency' component={CurrencyContainer}/>
            </Switch>
        </div>
    </Router>
)

export default Main;

