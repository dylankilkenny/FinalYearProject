import React, { Component } from 'react'
import { Sidebar, Segment, Button, Menu, Image, Icon, Header } from 'semantic-ui-react'

export default Selection = ({ visible, children }) => (
  <div>
        <Sidebar.Pushable as={Segment}>
          <Sidebar as={Menu} animation='uncover' width='thin' visible={visible} icon='labeled' vertical inverted>
            <Menu.Item name='home'>
              <Icon name='home' />
              Home
            </Menu.Item>
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
            <Segment basic>
              {children}
            </Segment>
          </Sidebar.Pusher>
        </Sidebar.Pushable>
      </div>
)
// class SidebarLeftUncover extends Component {
// 
//   render() {
//     const { visible } = this.props.visible;
//     return (
//       <div>
//         <Sidebar.Pushable as={Segment}>
//           <Sidebar as={Menu} animation='uncover' width='thin' visible={visible} icon='labeled' vertical inverted>
//             <Menu.Item name='home'>
//               <Icon name='home' />
//               Home
//             </Menu.Item>
//             <Menu.Item name='sign in'>
//               <Icon name='sign in' />
//               Sign in
//             </Menu.Item>
//             <Menu.Item name='mail'>
//               <Icon name='mail' />
//               Contact
//             </Menu.Item>
//           </Sidebar>
//           <Sidebar.Pusher>
//             <Segment basic>
//               {children}
//             </Segment>
//           </Sidebar.Pusher>
//         </Sidebar.Pushable>
//       </div>
//     )
//   }
// }

// export default SidebarLeftUncover