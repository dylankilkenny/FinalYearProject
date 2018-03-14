import React from 'react'
import { Header, Segment, Button, Icon } from 'semantic-ui-react'

const HeaderFloating = ({toggle}) => (
  <Segment clearing>
    <div>
    <Button floated='left' onClick={toggle}><Icon name='bars' /></Button>        
    </div>
    <div>
    <Header as='h2' textAlign='center'>
      CryptoAnalytics
    </Header>
    </div>
    
    
  </Segment>
)

export default HeaderFloating
