import React, { Component } from "react";
import ReactDOM from "react-dom";
import { Container } from 'semantic-ui-react'
import TableContainer from "./container/TableContainer.js";
import Header from "./presentational/Header.js";
import Menu from "./menu/Menu.js"

class Main extends Component {
  state = { 
    MenuVisible: false 
  }
  toggleVisibility = () => this.setState({ MenuVisible: !this.state.MenuVisible })

  render() {

    return (
        <div>
            <Menu visible={this.state.MenuVisible}>
              <Header toggle={this.toggleVisibility}/>
              <TableContainer />
            </Menu>
        </div>
    );
  }
}
export default Main;

// const wrapper = document.getElementById("create-article-form");
// wrapper ? ReactDOM.render(<FormContainer />, wrapper) : false;