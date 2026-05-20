import { LemonBanner, Link } from '@posthog/lemon-ui'

import { AI_TRAINING_URL } from './aiTrainingConstants'

const DISMISSAL_KEY = 'org-ai-training-announcement'

export function OrganizationAIAnnouncement(): JSX.Element {
    return (
        <LemonBanner type="info" dismissKey={DISMISSAL_KEY}>
            <strong>PostHog is training a model.</strong> We're building AI features that learn from anonymized
            aggregated data.{' '}
            <Link to={AI_TRAINING_URL} target="_blank">
                Find out more.
            </Link>
        </LemonBanner>
    )
}
