import React, { Component } from "react";
import ReactDOM from "react-dom";
import { Container } from 'semantic-ui-react'
import Header from "./presentational/Header.js";
import Menu from "./menu/Menu.js"
import Main from "./Main.js"

class App extends Component {
  state = { 
    MenuVisible: false 
  }
  toggleVisibility = () => this.setState({ MenuVisible: !this.state.MenuVisible })

  render() {

    return (
        <div>
            <Menu visible={this.state.MenuVisible}>
              <Header toggle={this.toggleVisibility}/>
              <Main/>
            </Menu>
        </div>
    );
  }
}
export default App;

// const wrapper = document.getElementById("create-article-form");
// wrapper ? ReactDOM.render(<FormContainer />, wrapper) : false;