import React from 'react'
import { Header, Grid, Segment, Button, Icon } from 'semantic-ui-react'
import Search from "../container/SearchContainer.js"

const HeaderFloating = ({ toggle }) => (
    <Segment>
        <Grid columns='equal'>
            <Grid.Column>
                <Button floated='left' onClick={toggle}><Icon name='bars' /></Button>
            </Grid.Column>
            <Grid.Column width={8}>
                <Header as='h2' textAlign='center'>
                    CryptoAnalytics
                </Header>
            </Grid.Column>
            <Grid.Column>
                <Search />
            </Grid.Column>
        </Grid>
    </Segment>

)

export default HeaderFloating
