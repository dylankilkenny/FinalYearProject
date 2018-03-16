import React, { Component } from "react";
import ReactDOM from "react-dom";
import { Switch, Route, BrowserRouter as Router } from 'react-router-dom'
import { Container } from 'semantic-ui-react'
import Header from "./presentational/Header.js";
import Menu from "./menu/Menu.js"
import MainSegContainer from "./container/MainSegContainer.js"
import TableContainer from "./container/TableContainer.js";
import CurrencyContainer from "./container/CurrencyContainer.js"

  


class App extends React.Component {
  state = { 
    MenuVisible: false 
  }
  toggleVisibility = () => this.setState({ MenuVisible: !this.state.MenuVisible })

  render() {

    return (
      <Router>
        <div  >
            <Switch>
                <Menu toggle={this.toggleVisibility} visible={this.state.MenuVisible}>
                  <Header toggle={this.toggleVisibility}/>
                  <Route exact path='/' component={TableContainer} />
                  <Route path='/currency/:currency' component={CurrencyContainer}/>
                </Menu>
            </Switch>
        </div>
    </Router>
    );
  }
}
export default App;

// const wrapper = document.getElementById("create-article-form");
// wrapper ? ReactDOM.render(<FormContainer />, wrapper) : false;