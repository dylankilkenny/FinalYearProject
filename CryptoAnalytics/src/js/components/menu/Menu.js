import React, { Component } from 'react'
import { Sidebar, Segment, Button, Menu, Image, Icon, Header } from 'semantic-ui-react'
import { Link } from 'react-router-dom'


export default Selection = ({ visible, children, toggle }) => (
  <div>
        <Sidebar.Pushable as={Segment}>
          <Sidebar as={Menu} animation='uncover' width='thin' visible={visible} icon='labeled' vertical inverted>
          <Link to={"/"} onClick={toggle}>
            <Menu.Item name='home'>
              <Icon name='home' />
              Home
            </Menu.Item>
            </Link>
            <Menu.Item name='sign in'>
              <Icon name='sign in' />
              Sign in
            </Menu.Item>
            <Menu.Item name='mail'>
              <Icon name='mail' />
              Contact
            </Menu.Item>
          </Sidebar>
          <Sidebar.Pusher>
            <Segment style={{minHeight: '100vh'}} >
              {children}
            </Segment>
          </Sidebar.Pusher>
        </Sidebar.Pushable>
      </div>
)